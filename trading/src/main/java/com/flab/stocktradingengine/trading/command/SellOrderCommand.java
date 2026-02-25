package com.flab.stocktradingengine.trading.command;

import java.math.BigDecimal;

/**
 * 매도 주문 명령 (trading 도메인 내부용)
 */
public record SellOrderCommand(
    Long accountId,
    String stockCode,
    String orderType,
    BigDecimal price,
    int quantity
) {
}
