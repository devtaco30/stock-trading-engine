package com.flab.stocktradingengine.market.view;

import java.math.BigDecimal;

/**
 * 시세 조회용 뷰 (API/외부 모듈 전달용)
 */
public record QuoteView(
    String stockCode,
    String stockName,
    BigDecimal currentPrice,
    BigDecimal previousClose,
    BigDecimal changeRate,
    BigDecimal open,
    BigDecimal high,
    BigDecimal low,
    Long volume
) {
}
