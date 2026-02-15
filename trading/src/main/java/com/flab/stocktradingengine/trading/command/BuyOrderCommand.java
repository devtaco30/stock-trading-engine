package com.flab.stocktradingengine.trading.command;

import java.math.BigDecimal;

/**
 * 매수 주문 명령 (trading 도메인 내부용)
 */
public record BuyOrderCommand(
    Long accountId,
    String stockCode,
    String orderType,
    BigDecimal price,
    int quantity
) {
}
