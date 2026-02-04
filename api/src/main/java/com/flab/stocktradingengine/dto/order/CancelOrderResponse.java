package com.flab.stocktradingengine.dto.order;

import lombok.Builder;

import java.math.BigDecimal;

/**
 * 주문 취소 응답 DTO
 */
@Builder
public record CancelOrderResponse(
    String orderId,
    BigDecimal returnedMargin // 매수 주문인 경우에만 존재
) {
}
