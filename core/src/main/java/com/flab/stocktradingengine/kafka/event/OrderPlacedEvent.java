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
    Instant orderAt
) {}
