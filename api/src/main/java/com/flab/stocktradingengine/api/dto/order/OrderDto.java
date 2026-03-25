package com.flab.stocktradingengine.api.dto.order;

import lombok.Builder;

import java.math.BigDecimal;

/**
 * 주문 목록 조회 응답 DTO
 */
@Builder
public record OrderDto(
    String orderId,
    String stockCode,
    String stockName,
    String side, // BUY | SELL
    String orderType, // LIMIT | MARKET
    BigDecimal price,
    Integer quantity,
    String status, // PENDING | FILLED | CANCELLED
    Long orderAt,
    Long filledAt // 체결된 경우
) {
}
