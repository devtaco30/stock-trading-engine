package com.flab.stocktradingengine.trading.kafka.event;

/**
 * 주문 취소 이벤트. orders.{stockCode} 토픽으로 발행.
 *
 * <p>API 서버가 DB 취소 처리 후 발행하고, 매칭 컨슈머가 수신해 OrderBook 에서 취소 마킹한다.</p>
 */
public record OrderCancelledEvent(
    Long orderId,
    String stockCode
) {}
