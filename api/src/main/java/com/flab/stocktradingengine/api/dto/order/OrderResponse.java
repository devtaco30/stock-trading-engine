package com.flab.stocktradingengine.api.dto.order;

import java.math.BigDecimal;

import com.flab.stocktradingengine.trading.view.PlaceOrderResultView;

import lombok.Builder;

/**
 * 주문 응답 DTO
 */
@Builder
public record OrderResponse(
    Long orderId,
    String status, // PENDING | FILLED | CANCELLED
    Long orderAt,
    BigDecimal reservedMargin // 매수 주문인 경우에만 존재
) {

    public static OrderResponse from(PlaceOrderResultView view) {
        return new OrderResponse(view.orderId(), view.status(), view.orderAt(), view.reservedMargin());
    }
}
