package com.flab.stocktradingengine.facade;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.dto.account.AccountDetailData;
import com.flab.stocktradingengine.dto.common.PagedResponse;
import com.flab.stocktradingengine.account.entity.Holding;
import com.flab.stocktradingengine.account.service.AccountService;
import com.flab.stocktradingengine.market.service.QuoteService;
import com.flab.stocktradingengine.market.view.StockInfo;
import com.flab.stocktradingengine.settlement.service.UnpaidQueryService;
import com.flab.stocktradingengine.trading.service.OrderQueryService;

import lombok.RequiredArgsConstructor;

/**
 * 계좌 조회 전용 퍼사드.
 * <p>다중 저장소에서 데이터만 수집·반환한다. 비즈니스 규칙·DTO 조립은 서비스에서 한다.</p>
 */
@Component
@RequiredArgsConstructor
public class AccountReadFacade {

    private final AccountService accountService;
    private final OrderQueryService orderQueryService;
    private final UnpaidQueryService unpaidQueryService;
    private final QuoteService quoteService;

    /**
     * 사용자 소유 계좌 목록 (account 모듈 서비스 경유, 목록 DTO 조립 시 사용).
     */
    public List<Account> getAccountsByUserId(Long userId) {
        return accountService.getAccountsByUserId(userId);
    }

    /**
     * 계좌 상세 조회용 수집 데이터 (증거금 합계, 미결제 합계, 보유 평가금 합계).
     */
    public AccountDetailData getAccountDetail(Long accountId) {
        BigDecimal reservedMarginSum = orderQueryService.getReservedMarginSumByAccountId(accountId);
        BigDecimal unpaidSum = unpaidQueryService.getPendingUnpaidSumByAccountId(accountId);

        List<Holding> holdings = accountService.getHoldingsPage(accountId, Pageable.unpaged()).getData();
        List<String> stockCodes = holdings.stream().map(Holding::getStockCode).distinct().toList();
        Map<String, StockInfo> stockInfoMap = quoteService.getStockInfoBatch(stockCodes);

        BigDecimal totalAssetsFromHoldings = BigDecimal.ZERO;
        for (Holding h : holdings) {
            BigDecimal currentPrice = stockInfoMap.getOrDefault(h.getStockCode(), new StockInfo(h.getStockCode(), BigDecimal.ZERO)).currentPrice();
            if (currentPrice.compareTo(BigDecimal.ZERO) == 0) {
                currentPrice = h.getAveragePrice();
            }
            totalAssetsFromHoldings = totalAssetsFromHoldings.add(currentPrice.multiply(BigDecimal.valueOf(h.getQuantity())));
        }

        return new AccountDetailData(reservedMarginSum, unpaidSum, totalAssetsFromHoldings);
    }

    /**
     * 보유 종목 페이지. account 모듈 서비스 경유.
     */
    public PagedResponse<Holding> getHoldingsPage(Long accountId, Pageable pageable) {
        return accountService.getHoldingsPage(accountId, pageable);
    }

    /**
     * 종목 코드별 종목명·현재가. 종목 없으면 empty.
     */
    public Optional<StockInfo> getStockInfo(String stockCode) {
        return quoteService.getStockInfo(stockCode);
    }

    /**
     * 종목 코드 목록별 StockInfo 일괄 조회. N+1 방지용.
     */
    public Map<String, StockInfo> getStockInfoBatch(List<String> stockCodes) {
        return quoteService.getStockInfoBatch(stockCodes);
    }
}
