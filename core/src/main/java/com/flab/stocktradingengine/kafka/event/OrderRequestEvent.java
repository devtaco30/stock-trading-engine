package com.flab.stocktradingengine.kafka.event;

import java.math.BigDecimal;
import java.time.Instant;

import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * 주문 접수 요청 이벤트. order-requests 토픽으로 발행.
 *
 * <p>API 서버가 가격 제한폭·계좌 검증 후 발행한다.
 * order-engine 이 수신해 잔고·보유 검증 후 주문을 DB 에 저장한다.</p>
 */
public record OrderRequestEvent(
    Long accountId,
    String stockCode,
    OrderSide side,
    String orderType,
    BigDecimal price,
    int quantity,
    Instant requestedAt
) {}
