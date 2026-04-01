package com.flab.stocktradingengine.trading.kafka.event;

/**
 * 주문 취소 요청 이벤트. order-requests 토픽으로 발행.
 *
 * <p>API 서버가 주문 조회·계좌 소유 검증 후 발행한다.
 * trading/OrderRequestConsumer 가 수신해 DB 상태를 CANCELLED 로 전환하고
 * orders.{stockCode} 에 OrderCancelledEvent 를 발행한다.</p>
 */
public record OrderCancelRequestEvent(
    Long accountId,
    Long orderId,
    String stockCode
) {}
