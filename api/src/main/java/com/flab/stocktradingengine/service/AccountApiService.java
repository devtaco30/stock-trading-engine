package com.flab.stocktradingengine.service;

import static com.flab.stocktradingengine.mapper.HoldingDtoMapper.toHoldingDto;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.account.entity.Holding;
import com.flab.stocktradingengine.dto.account.response.AccountDetailDto;
import com.flab.stocktradingengine.dto.account.response.AccountDto;
import com.flab.stocktradingengine.dto.account.response.HoldingDto;
import com.flab.stocktradingengine.dto.account.response.TransactionResponse;
import com.flab.stocktradingengine.dto.common.PagedResponse;
import com.flab.stocktradingengine.facade.AccountCommandFacade;
import com.flab.stocktradingengine.dto.account.AccountDetailData;
import com.flab.stocktradingengine.facade.AccountReadFacade;
import com.flab.stocktradingengine.mapper.AccountDetailDtoMapper;
import com.flab.stocktradingengine.mapper.AccountDtoMapper;
import com.flab.stocktradingengine.market.view.StockInfo;
import com.flab.stocktradingengine.resolver.AccountAccessResolver;

import lombok.RequiredArgsConstructor;

/**
 * 계좌 API 서비스 (api 모듈).
 * <p>
 * 비즈니스 규칙·오케스트레이션·DTO 조립을 담당한다. 데이터 수집은 {@link AccountReadFacade}에 위임.
 * </p>
 */
@Service
@RequiredArgsConstructor
public class AccountApiService {

    private final AccountReadFacade accountReadFacade;
    private final AccountAccessResolver accountAccessResolver;
    private final AccountCommandFacade accountCommandFacade;

    /**
     * 계좌 상세 조회. 본인 소유 계좌만 허용하며, 타인 계좌 요청 시 403.
     * 한 메서드 내 여러 조회가 같은 스냅샷을 보도록 readOnly 트랜잭션 사용.
     */
    @Transactional(readOnly = true)
    public AccountDetailDto getAccountDetail(Long userId, Long accountId) {
        return accountAccessResolver.resolveAccountOwnedBy(userId, accountId)
                .map(account -> {
                    AccountDetailData data = accountReadFacade.getAccountDetail(accountId);
                    return AccountDetailDtoMapper.toDetailDto(account, data);
                })
                .orElse(null);
    }

    /**
     * 계좌 목록 조회 (해당 사용자 소유 계좌만).
     * 한 메서드 내 여러 조회가 같은 스냅샷을 보도록 readOnly 트랜잭션 사용.
     */
    @Transactional(readOnly = true)
    public List<AccountDto> getAccounts(Long userId) {
        return accountReadFacade.getAccountsByUserId(userId).stream()
                .map(account -> {
                    AccountDetailData data = accountReadFacade.getAccountDetail(account.getAccountId());
                    return AccountDtoMapper.toAccountDto(account, data);
                })
                .collect(Collectors.toList());
    }

    /**
     * 보유 주식 조회 (페이지네이션). 본인 소유 계좌만 허용, 타인 계좌 요청 시 403.
     * 한 메서드 내 여러 조회가 같은 스냅샷을 보도록 readOnly 트랜잭션 사용.
     */
    @Transactional(readOnly = true)
    public PagedResponse<HoldingDto> getHoldingsPaged(Long userId, Long accountId, int page, int size) {
        accountAccessResolver.resolveAccountOwnedBy(userId, accountId);

        Pageable pageable = PageRequest.of(page, size);

        PagedResponse<Holding> holdingPaged = accountReadFacade.getHoldingsPage(accountId, pageable);
        List<Holding> content = holdingPaged.getData();
        List<String> stockCodes = content.stream().map(Holding::getStockCode).distinct().toList();
        Map<String, StockInfo> stockInfoMap = accountReadFacade.getStockInfoBatch(stockCodes);


        List<HoldingDto> holdings = content.stream()
                .map(h -> toHoldingDto(h, stockInfoMap.getOrDefault(h.getStockCode(),
                    new StockInfo(h.getStockCode(), BigDecimal.ZERO, BigDecimal.ZERO))))
                .collect(Collectors.toList());

        return PagedResponse.of(holdings, holdingPaged.getPagination());
    }

    /**
     * 입금. 소유·ACTIVE 검증 후 계좌 쓰기는 퍼사드 경유.
     * Resolver 검증과 쓰기를 한 트랜잭션으로 묶어 정합성(같은 스냅샷)과 효율(커넥션 1회 사용) 확보.
     */
    @Transactional
    public TransactionResponse deposit(Long userId, Long accountId, BigDecimal amount) {
        accountAccessResolver.resolveAccountOwnedAndActive(userId, accountId);
        BigDecimal balance = accountCommandFacade.deposit(accountId, amount);
        return TransactionResponse.builder()
            .balance(balance)
            .respondedAt(System.currentTimeMillis())
            .build();
    }

    /**
     * 출금. 소유·ACTIVE 검증 후 계좌 쓰기는 퍼사드 경유.
     * Resolver 검증과 쓰기를 한 트랜잭션으로 묶어 정합성(같은 스냅샷)과 효율(커넥션 1회 사용) 확보.
     */
    @Transactional
    public TransactionResponse withdraw(Long userId, Long accountId, BigDecimal amount) {
        accountAccessResolver.resolveAccountOwnedAndActive(userId, accountId);
        BigDecimal balance = accountCommandFacade.withdraw(accountId, amount);
        return TransactionResponse.builder()
            .balance(balance)
            .respondedAt(System.currentTimeMillis())
            .build();
    }
}
