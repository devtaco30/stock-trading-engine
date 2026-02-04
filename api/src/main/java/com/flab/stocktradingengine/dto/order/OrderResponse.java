package com.flab.stocktradingengine.dto.order;

import lombok.Builder;

import java.math.BigDecimal;

/**
 * 주문 응답 DTO
 */
@Builder
public record OrderResponse(
    String orderId,
    String status, // PENDING | FILLED | CANCELLED
    Long orderAt,
    BigDecimal heldMargin // 매수 주문인 경우에만 존재
) {
}
