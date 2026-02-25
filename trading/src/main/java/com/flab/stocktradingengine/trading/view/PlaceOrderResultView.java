package com.flab.stocktradingengine.trading.view;

import java.math.BigDecimal;

/**
 * 주문 접수 결과 뷰 (API/외부 모듈 전달용)
 */
public record PlaceOrderResultView(
    Long orderId,
    String status,
    Long orderAt,
    BigDecimal reservedMargin
) {
}
