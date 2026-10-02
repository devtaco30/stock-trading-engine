package com.flab.stocktradingengine.kafka.event;

import java.math.BigDecimal;
import java.time.Instant;

import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * 주문 접수 이벤트. orders.{stockCode} 토픽으로 발행.
 *
 * <p>API 서버가 DB 커밋 후 발행하고, 매칭 컨슈머가 수신해 OrderBook 에 추가한다.</p>
 */
public record OrderPlacedEvent(
    Long orderId,
    Long accountId,
    String stockCode,
    OrderSide side,
    BigDecimal price,
    int quantity,
    Instant orderAt,
    long receivedAtNanos,
    long acceptedAtNanos
) {

    /**
     * 구간 분해 측정(measure/segments 브랜치 한정)용 시각 필드를 지정하지 않는 생성자 — 두 필드는 0이다.
     * {@code orderAt}은 api가 요청을 받은 시각(requestedAt)이라 측정의 시작 시각(V-P0)으로 쓴다.
     * {@code receivedAtNanos}는 order-engine 컨슈머가 레코드를 받은 시각(V-P1),
     * {@code acceptedAtNanos}는 DB 커밋까지 끝난 접수 완료 시각(V-P7)이다.
     */
    public OrderPlacedEvent(Long orderId, Long accountId, String stockCode, OrderSide side,
                            BigDecimal price, int quantity, Instant orderAt) {
        this(orderId, accountId, stockCode, side, price, quantity, orderAt, 0L, 0L);
    }
}
