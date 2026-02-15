package com.flab.stocktradingengine.trading.view;

import java.math.BigDecimal;

/**
 * 주문 취소 결과 뷰 (API/외부 모듈 전달용)
 */
public record CancelOrderResultView(
    Long orderId,
    BigDecimal returnedMargin
) {
}
