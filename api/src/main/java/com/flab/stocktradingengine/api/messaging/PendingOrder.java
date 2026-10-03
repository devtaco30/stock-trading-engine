package com.flab.stocktradingengine.api.messaging;

import java.math.BigDecimal;

import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * 전달 큐에 담는 검증이 끝난 주문. {@code originNanos}는 HTTP 스레드가 큐에 넣기 직전에 찍은 시각이고(v1의 {@code requestedAt}과 같은 위치),
 * 끝점① 지연의 시작 시각이다.
 */
public record PendingOrder(
    OrderSide side,
    long accountId,
    String stockCode,
    BigDecimal price,
    int quantity,
    String requestId,
    long originNanos
) {}
