package com.flab.stocktradingengine.market.view;

import java.math.BigDecimal;

/**
 * 종목 검색 결과 뷰 (API/외부 모듈 전달용)
 */
public record StockSearchView(
    String stockCode,
    String stockName,
    BigDecimal currentPrice
) {
}
