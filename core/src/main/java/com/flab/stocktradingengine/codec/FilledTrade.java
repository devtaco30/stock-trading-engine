package com.flab.stocktradingengine.codec;

import java.math.BigDecimal;

/**
 * 체결 하나의 불변 스냅샷 — Aeron 와이어 인코딩/디코딩 결과를 담는다.
 * 매칭·계좌 양쪽이 함께 보는 공유 계약이라 core에 둔다.
 *
 * <p>Kafka {@code TradeFilledEvent}(core/kafka/event)와 필드는 같지만 codec 패키지에 별도로 둔다 —
 * 코덱이 kafka 패키지에 의존하지 않게 하기 위함이다({@code TradeFilledEvent}는 유닛 5에서 제거된다).
 * {@code JournaledOrder}가 {@code OrderCodec}의 디코딩 대상인 것과 같은 자리다.</p>
 */
public record FilledTrade(
    long tradeId,
    String stockCode,
    long buyOrderId,
    long buyAccountId,
    long sellOrderId,
    long sellAccountId,
    int filledQuantity,
    BigDecimal matchPrice
) {
}
