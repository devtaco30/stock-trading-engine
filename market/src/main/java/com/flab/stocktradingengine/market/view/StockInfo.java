package com.flab.stocktradingengine.market.view;

import java.math.BigDecimal;

/**
 * 종목 코드에 대한 주식 정보 (종목명, 현재가).
 */
public record StockInfo(
    String stockName,
    BigDecimal currentPrice
) {
}
