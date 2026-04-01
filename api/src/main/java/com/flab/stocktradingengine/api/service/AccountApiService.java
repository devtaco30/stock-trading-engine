package com.flab.stocktradingengine.api.service;

import static com.flab.stocktradingengine.api.mapper.HoldingDtoMapper.toHoldingDto;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.account.entity.Holding;
import com.flab.stocktradingengine.account.service.AccountService;
import com.flab.stocktradingengine.api.dto.account.AccountDetailData;
import com.flab.stocktradingengine.api.dto.account.AccountDetailDto;
import com.flab.stocktradingengine.api.dto.account.AccountDto;
import com.flab.stocktradingengine.api.dto.account.HoldingDto;
import com.flab.stocktradingengine.api.dto.account.TransactionResponse;
import com.flab.stocktradingengine.api.mapper.AccountDetailDtoMapper;
import com.flab.stocktradingengine.api.mapper.AccountDtoMapper;
import com.flab.stocktradingengine.api.resolver.AccountAccessResolver;
import com.flab.stocktradingengine.dto.common.PagedResponse;
import com.flab.stocktradingengine.exception.ResourceNotFoundException;
import com.flab.stocktradingengine.market.service.QuoteService;
import com.flab.stocktradingengine.market.view.StockInfo;
import com.flab.stocktradingengine.settlement.service.UnpaidQueryService;
import com.flab.stocktradingengine.trading.service.OrderQueryService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 계좌 API 서비스 (api 모듈).
 * <p>비즈니스 규칙·오케스트레이션·DTO 조립을 담당한다.</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AccountApiService {

    private final AccountService accountService;
    private final AccountAccessResolver accountAccessResolver;
    private final OrderQueryService orderQueryService;
    private final UnpaidQueryService unpaidQueryService;
    private final QuoteService quoteService;

    /**
     * 계좌 상세 조회. 본인 소유 계좌만 허용하며, 타인 계좌 요청 시 403.
     */
    @Transactional(readOnly = true)
    public AccountDetailDto getAccountDetail(Long userId, Long accountId) {
        Account account = accountAccessResolver.resolveAccountOwnedBy(userId, accountId)
                .orElseThrow(() -> new ResourceNotFoundException("Account not found: " + accountId));
        AccountDetailData data = buildAccountDetailData(accountId);
        return AccountDetailDtoMapper.toDetailDto(account, data);
    }

    /**
     * 계좌 목록 조회 (해당 사용자 소유 계좌만).
     */
    public List<AccountDto> getAccounts(Long userId) {
        return accountService.getAccountsByUserId(userId).stream()
                .map(AccountDtoMapper::toAccountDto)
                .collect(Collectors.toList());
    }

    /**
     * 보유 주식 조회 (페이지네이션). 본인 소유 계좌만 허용, 타인 계좌 요청 시 403.
     */
    @Transactional(readOnly = true)
    public PagedResponse<HoldingDto> getHoldingsPaged(Long userId, Long accountId, int page, int size) {
        // 본인 소유 계좌만 허용
        accountAccessResolver.resolveAccountOwnedBy(userId, accountId);

        // 페이지네이션 정보 생성
        Pageable pageable = PageRequest.of(page, size);

        // 보유 종목 조회
        PagedResponse<Holding> holdingPaged = accountService.getHoldingsPage(accountId, pageable);

        // 보유 종목 정보 조회
        List<Holding> content = holdingPaged.getData();
        List<String> stockCodes = content.stream().map(Holding::getStockCode).distinct().toList();
        Map<String, StockInfo> stockInfoMap = quoteService.getStockInfoBatch(stockCodes);


        List<HoldingDto> holdings = content.stream()
                .map(h -> toHoldingDto(h, stockInfoMap.getOrDefault(h.getStockCode(),
                    new StockInfo(h.getStockCode(), BigDecimal.ZERO, BigDecimal.ZERO))))
                .collect(Collectors.toList());

        return PagedResponse.of(holdings, holdingPaged.getPagination());
    }

    /**
     * 입금. 소유·ACTIVE 검증 후 잔액 반영.
     * Resolver 검증과 쓰기를 한 트랜잭션으로 묶어 정합성과 효율(커넥션 1회) 확보.
     */
    @Transactional
    public TransactionResponse deposit(Long userId, Long accountId, BigDecimal amount) {
        accountAccessResolver.resolveAccountOwnedAndActive(userId, accountId);
        BigDecimal balance = accountService.deposit(accountId, amount);
        return TransactionResponse.builder()
            .balance(balance)
            .respondedAt(System.currentTimeMillis())
            .build();
    }

    /**
     * 출금. 소유·ACTIVE 검증 후 잔액 반영.
     * Resolver 검증과 쓰기를 한 트랜잭션으로 묶어 정합성과 효율(커넥션 1회) 확보.
     */
    @Transactional
    public TransactionResponse withdraw(Long userId, Long accountId, BigDecimal amount) {
        accountAccessResolver.resolveAccountOwnedAndActive(userId, accountId);
        BigDecimal balance = accountService.withdraw(accountId, amount);
        return TransactionResponse.builder()
            .balance(balance)
            .respondedAt(System.currentTimeMillis())
            .build();
    }

    // ── private helpers ───────────────────────────────────────────────────────

    private AccountDetailData buildAccountDetailData(Long accountId) {
        BigDecimal reservedMarginSum = orderQueryService.getReservedMarginSumByAccountId(accountId);
        BigDecimal unpaidSum = unpaidQueryService.getPendingUnpaidSumByAccountId(accountId);
        List<Holding> holdings = accountService.getHoldingsPage(accountId, Pageable.unpaged()).getData();
        List<String> stockCodes = holdings.stream().map(Holding::getStockCode).distinct().toList();
        Map<String, StockInfo> stockInfoMap = quoteService.getStockInfoBatch(stockCodes);

        BigDecimal totalAssetsFromHoldings = BigDecimal.ZERO;
        for (Holding h : holdings) {
            StockInfo info = stockInfoMap.get(h.getStockCode());
            BigDecimal currentPrice;
            if (info != null && info.currentPrice().compareTo(BigDecimal.ZERO) > 0) {
                currentPrice = info.currentPrice();
            } else if (info != null) {
                currentPrice = info.previousClose();
            } else {
                log.warn("[평가금 계산] 종목 정보 없음 — 데이터 정합성 오류: stockCode={}", h.getStockCode());
                currentPrice = BigDecimal.ZERO;
            }
            totalAssetsFromHoldings = totalAssetsFromHoldings.add(
                currentPrice.multiply(BigDecimal.valueOf(h.getQuantity())));
        }

        return new AccountDetailData(reservedMarginSum, unpaidSum, totalAssetsFromHoldings);
    }
}
