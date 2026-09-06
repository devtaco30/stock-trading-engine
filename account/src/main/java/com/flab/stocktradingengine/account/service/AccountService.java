package com.flab.stocktradingengine.account.service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.dto.common.PagedResponse;
import com.flab.stocktradingengine.account.entity.AccountStatus;
import com.flab.stocktradingengine.account.entity.AccountStatusHistory;
import com.flab.stocktradingengine.account.entity.Holding;
import com.flab.stocktradingengine.account.repository.AccountRepository;
import com.flab.stocktradingengine.user.entity.User;
import com.flab.stocktradingengine.account.repository.AccountStatusHistoryRepository;
import com.flab.stocktradingengine.account.exception.InsufficientResourceException;
import com.flab.stocktradingengine.account.repository.HoldingRepository;
import com.flab.stocktradingengine.exception.ResourceNotFoundException;
import com.flab.stocktradingengine.exception.InvalidRequestException;

import lombok.RequiredArgsConstructor;

/**
 * 계좌 도메인 서비스 (account 모듈).
 * <p>계좌·보유 종목 조회, 상태 변경, 매수 체결 시 보유 반영 등 계좌 도메인 로직을 담당한다.</p>
 */
@Service
@RequiredArgsConstructor
public class AccountService {

    private final AccountRepository accountRepository;
    private final HoldingRepository holdingRepository;
    private final AccountStatusHistoryRepository accountStatusHistoryRepository;
    

    /**
     * 특정 계좌 조회.
     * 단일 repository 호출만 하므로 서비스 레벨 트랜잭션 없음(Repository에서 트랜잭션 처리).
     */
    public Optional<Account> getAccount(Long accountId) {
        return accountRepository.findByAccountId(accountId);
    }

    /**
     * 계좌 행 락 (SELECT FOR UPDATE). Snowflake accountId 기반.
     * 락은 트랜잭션 종료 시까지 유지되므로 호출자가 같은 트랜잭션 내에서 갱신할 때만 의미 있음.
     */
    @Transactional
    public Optional<Account> getAccountByAccountIdForUpdate(Long accountId) {
        return accountRepository.findByAccountIdForUpdate(accountId);
    }

    /**
     * 해당 종목 보유 행 락 (SELECT FOR UPDATE). Snowflake accountId 기반.
     * 락 유지를 위해 트랜잭션 필수.
     */
    @Transactional
    public Optional<Holding> getHoldingByAccountIdForUpdate(Long accountId, String stockCode) {
        return holdingRepository.findByAccount_AccountIdAndStockCodeForUpdate(accountId, stockCode);
    }

    /**
     * 사용자 소유 계좌 목록 조회. 인증된 사용자의 계좌만 반환.
     * 단일 repository 호출만 하므로 서비스 레벨 트랜잭션 없음.
     */
    public List<Account> getAccountsByUserId(Long userId) {
        return accountRepository.findByUser_Id(userId);
    }

    /**
     * 계좌 상태 변경. 전이 규칙 검증 후 변경하고 이력을 남긴다.
     * 계좌 UPDATE와 이력 INSERT가 원자적으로 처리되어야 하므로 트랜잭션 필수.
     *
     * @param changedByUser null이면 시스템에 의한 변경
     */
    @Transactional
    public void changeStatus(Long accountId, AccountStatus toStatus, User changedByUser, String reason) {
        Account account = accountRepository.findByAccountId(accountId)
            .orElseThrow(() -> new ResourceNotFoundException("Account not found: " + accountId));

        AccountStatus fromStatus = account.getStatus();
        if (!fromStatus.canTransitionTo(toStatus)) {
            throw new InvalidRequestException(
                "Invalid status transition: " + fromStatus + " -> " + toStatus);
        }

        account.changeStatus(toStatus);
        accountStatusHistoryRepository.save(
            new AccountStatusHistory(account, fromStatus, toStatus, changedByUser, reason)
        );
    }

    /**
     * 계좌별 보유 종목 목록 페이징 조회.
     * 단일 repository 호출만 하므로 서비스 레벨 트랜잭션 없음.
     */
    public PagedResponse<Holding> getHoldingsPage(Long accountId, Pageable pageable) {
        Page<Holding> page = holdingRepository.findByAccount_AccountId(accountId, pageable);
        return PagedResponse.of(page.getContent(), page.getNumber(), page.getSize(), page.getTotalElements());
    }

    /**
     * 입금. 계좌 잔액에 금액을 더한다. 동시 입출금 방지를 위해 비관적 락 사용.
     * 같은 트랜잭션 내에서 조회(락) → 검증 → 갱신이 원자적으로 이루어져야 하므로 트랜잭션 필수.
     *
     * @return 반영 후 잔액
     */
    @Transactional
    public BigDecimal deposit(Long accountId, BigDecimal amount) {
        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new InvalidRequestException("입금 금액은 0보다 커야 합니다.");
        }
        Account account = accountRepository.findByAccountIdForUpdate(accountId)
            .orElseThrow(() -> new ResourceNotFoundException("Account not found: " + accountId));
        BigDecimal newBalance = account.getBalance().add(amount);
        account.changeBalance(newBalance);
        return newBalance;
    }

    /**
     * 출금(시스템 차감용, 예: 정산 시 실제 대금 결제). 예약증거금·미결제를 고려하지 않고
     * 잔고 전액을 가용으로 본다. 이미 성립한 체결의 결제이므로 예약으로 막으면 안 된다.
     *
     * @return 반영 후 잔액
     */
    @Transactional
    public BigDecimal withdraw(Long accountId, BigDecimal amount) {
        return withdraw(accountId, amount, () -> BigDecimal.ZERO);
    }

    /**
     * 유저 출금. 가용잔고(잔고 - committed) 기준으로 검증한다.
     * committed = 예약증거금(PENDING 매수) + 미결제 미수금 합. 매수 접수와 대칭으로
     * 계좌 락을 보유한 채 committedSupplier 를 호출해 가용을 계산하므로 검증~차감이 원자적이다.
     * 예약·미결제에 묶인 현금은 출금할 수 없다.
     *
     * @param committedSupplier 락 획득 후 호출되는 예약증거금+미결제 합계 공급자
     * @return 반영 후 잔액
     */
    @Transactional
    public BigDecimal withdraw(Long accountId, BigDecimal amount, Supplier<BigDecimal> committedSupplier) {
        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new InvalidRequestException("출금 금액은 0보다 커야 합니다.");
        }
        Account account = accountRepository.findByAccountIdForUpdate(accountId)
            .orElseThrow(() -> new ResourceNotFoundException("Account not found: " + accountId));
        BigDecimal committed = committedSupplier.get();
        if (committed == null) {
            committed = BigDecimal.ZERO;
        }
        BigDecimal available = account.getBalance().subtract(committed);
        if (amount.compareTo(available) > 0) {
            throw new InsufficientResourceException("가용잔고 초과: 요청=" + amount + " 가용=" + available);
        }
        BigDecimal newBalance = account.getBalance().subtract(amount);
        account.changeBalance(newBalance);
        return newBalance;
    }

    /**
     * 매도 체결 시 보유 수량 차감. 보유가 없으면 예외.
     */
    @Transactional
    public void decreaseHolding(Long accountId, String stockCode, int quantity) {
        // 비관적 락(ForUpdate)으로 조회 — 조회~차감 사이 동시 갱신에 의한 Lost Update 방지.
        // 다른 보유 갱신 메서드(addHoldingOrIncreaseQuantity)와 락 전략 일관.
        Holding holding = holdingRepository.findByAccount_AccountIdAndStockCodeForUpdate(accountId, stockCode)
            .orElseThrow(() -> new ResourceNotFoundException("Holding not found: accountId=" + accountId + ", stockCode=" + stockCode));
        holding.subtractQuantity(quantity);
    }

    /**
     * 매수 체결 시 보유 반영. 해당 종목이 없으면 새로 추가, 있으면 수량·평균가 합산.
     */
    @Transactional
    public void addHoldingOrIncreaseQuantity(Long accountId, String stockCode, int quantity, BigDecimal executionPrice) {
        Account account = accountRepository.findByAccountId(accountId)
            .orElseThrow(() -> new ResourceNotFoundException("Account not found: " + accountId));

        holdingRepository.findByAccount_IdAndStockCodeForUpdate(account.getId(), stockCode)
            .ifPresentOrElse(
                holding -> holding.applyExecution(quantity, executionPrice),
                () -> holdingRepository.save(new Holding(account, stockCode, quantity, executionPrice))
            );
    }
}
