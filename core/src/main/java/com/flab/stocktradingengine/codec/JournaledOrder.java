package com.flab.stocktradingengine.codec;

import java.math.BigDecimal;
import java.time.Instant;

import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * 매칭 인테이크 주문의 불변 스냅샷 — 저널 기록과 Aeron 와이어 디코딩 결과를 겸한다.
 * 계좌·매칭 양쪽이 함께 보는 공유 계약이라 core에 둔다.
 *
 * <p>{@code matching-disruptor}의 {@code OrderEvent}(링버퍼가 재사용하는 가변 슬롯)를 그대로 저널에
 * 담으면 다음 발행 때 필드가 덮어써져 기록이 훼손된다 — 그래서 저널러가 발행 시점의 값을 이 불변
 * record로 복사해 담는다({@code OrderEvent}는 매칭 코어 전용 타입이라 여기서 그 변환은 하지 않는다,
 * 호출부(matching-disruptor의 {@code JournalEventHandler})가 직접 필드를 옮겨 담는다).</p>
 */
public record JournaledOrder(
    EventType type,
    long orderId,
    long accountId,
    String stockCode,
    OrderSide side,
    BigDecimal price,
    int quantity,
    Instant orderAt
) {
}
