package com.flab.stocktradingengine.facade;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.account.entity.Holding;
import com.flab.stocktradingengine.account.service.AccountService;
import com.flab.stocktradingengine.dto.account.AccountDetailData;
import com.flab.stocktradingengine.dto.common.PagedResponse;
import com.flab.stocktradingengine.market.service.QuoteService;
import com.flab.stocktradingengine.market.view.StockInfo;
import com.flab.stocktradingengine.settlement.service.UnpaidQueryService;
import com.flab.stocktradingengine.trading.service.OrderQueryService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 계좌 조회 전용 퍼사드.
 * <p>다중 저장소에서 데이터만 수집·반환한다. 비즈니스 규칙·DTO 조립은 서비스에서 한다.</p>
 */
@Slf4j
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
     * 
     * 기본적으로 readOnly 이다. 
     * OrderCommandService 에서 호출 될 경우에는 해당 Transaction 에 참여한다. (Write)
     * 
     * Transactional 을 선언한 이유는 단순 read 가 아니라, 여러 조회가 같은 스냅샷을 보도록 하기 위해서이다.
     */
    @Transactional(readOnly = true) 
    public AccountDetailData getAccountDetail(Long accountId) {

        // 예약 증거금 합계
        BigDecimal reservedMarginSum = orderQueryService.getReservedMarginSumByAccountId(accountId);

        // 미결제 미수금 합계
        BigDecimal unpaidSum = unpaidQueryService.getPendingUnpaidSumByAccountId(accountId);

        // 보유 종목 목록 조회
        List<Holding> holdings = accountService.getHoldingsPage(accountId, Pageable.unpaged()).getData();

        // 종목 코드 목록 조회
        List<String> stockCodes = holdings.stream().map(Holding::getStockCode).distinct().toList();

        // 종목 정보 조회
        Map<String, StockInfo> stockInfoMap = quoteService.getStockInfoBatch(stockCodes);

        // 보유 평가금 합계
        BigDecimal totalAssetsFromHoldings = BigDecimal.ZERO;

        // 보유 종목 평가금 합계 계산
        for (Holding h : holdings) {
            StockInfo info = stockInfoMap.get(h.getStockCode());
            BigDecimal currentPrice;
            if (info != null && info.currentPrice().compareTo(BigDecimal.ZERO) > 0) {
                // 정상: 현재가 사용
                currentPrice = info.currentPrice();
            } else if (info != null) {
                // 현재가 없음 (장 종료 등): 전일 종가로 대체 (previousClose는 nullable = false)
                currentPrice = info.previousClose();
            } else {
                // StockInfo 자체 없음: 보유 종목인데 Stock이 없다는 건 데이터 정합성 오류
                // 잘못된 숫자(averagePrice)보다 0 처리 후 운영자가 확인하도록 경고
                log.warn("[평가금 계산] 종목 정보 없음 — 데이터 정합성 오류: stockCode={}", h.getStockCode());
                currentPrice = BigDecimal.ZERO;
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
