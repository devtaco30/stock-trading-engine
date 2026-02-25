package com.flab.stocktradingengine.dto.order;

import java.math.BigDecimal;

import com.flab.stocktradingengine.trading.view.CancelOrderResultView;

import lombok.Builder;

/**
 * 주문 취소 응답 DTO
 */
@Builder
public record CancelOrderResponse(
    Long orderId,
    BigDecimal returnedMargin // 매수 주문인 경우에만 존재
) {

    public static CancelOrderResponse from(CancelOrderResultView view) {
        return new CancelOrderResponse(view.orderId(), view.returnedMargin());
    }
}
