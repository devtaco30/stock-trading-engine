package com.flab.stocktradingengine.kafka.event;

import java.math.BigDecimal;

/**
 * 매칭 엔진이 체결을 완료했을 때 fills.{stockCode} 토픽으로 발행하는 이벤트.
 *
 * <p>matching-engine → settlement-engine 간 Kafka 메시지로 사용된다.</p>
 */
public record TradeFilledEvent(
    Long tradeId,
    String stockCode,
    Long buyOrderId,
    Long buyAccountId,
    Long sellOrderId,
    Long sellAccountId,
    int filledQuantity,
    BigDecimal matchPrice,
    long emittedAtNanos,
    long aggressorOriginNanos
) {

    /**
     * 구간 분해 측정(measure/segments 브랜치 한정)용 시각 필드를 지정하지 않는 생성자 — 두 필드는 0이다.
     * {@code emittedAtNanos}는 매칭이 체결 이벤트를 방출하기 직전 시각(V-P10),
     * {@code aggressorOriginNanos}는 이 체결을 일으킨 주문의 api 요청 시각(V-P0)이다.
     */
    public TradeFilledEvent(Long tradeId, String stockCode, Long buyOrderId, Long buyAccountId,
                            Long sellOrderId, Long sellAccountId, int filledQuantity, BigDecimal matchPrice) {
        this(tradeId, stockCode, buyOrderId, buyAccountId, sellOrderId, sellAccountId, filledQuantity, matchPrice, 0L, 0L);
    }
}
