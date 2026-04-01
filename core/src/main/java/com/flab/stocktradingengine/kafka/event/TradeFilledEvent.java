package com.flab.stocktradingengine.kafka.event;

import java.math.BigDecimal;

/**
 * 매칭 엔진이 체결을 완료했을 때 fills.{stockCode} 토픽으로 발행하는 이벤트.
 *
 * <p>matching-engine → settlement-engine 간 Kafka 메시지로 사용된다.</p>
 */
public record TradeFilledEvent(
    String stockCode,
    Long buyOrderId,
    Long buyAccountId,
    Long sellOrderId,
    Long sellAccountId,
    int filledQuantity,
    BigDecimal matchPrice
) {}
