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
    Instant orderAt,
    long originNanos,
    long receivedAtNanos,
    long acceptedAtNanos,
    int stockSlot
) {

    /** stockSlot을 지정하지 않는 호출부용 생성자 — 주문 항목(PLACE·CANCEL)은 종목 slot 번호를 담지 않는다(0). */
    public JournaledOrder(
        EventType type, long orderId, long accountId, String stockCode,
        OrderSide side, BigDecimal price, int quantity, Instant orderAt,
        long originNanos, long receivedAtNanos, long acceptedAtNanos
    ) {
        this(type, orderId, accountId, stockCode, side, price, quantity, orderAt,
            originNanos, receivedAtNanos, acceptedAtNanos, 0);
    }

    /** 종목 slot을 받았다는 저널 항목. */
    public static JournaledOrder slotAcquired(int stockSlot) {
        return new JournaledOrder(EventType.SLOT_ACQUIRED, 0L, 0L, null, null, null, 0, null, 0L, 0L, 0L, stockSlot);
    }

    /** 종목 slot을 넘겼다는 저널 항목. */
    public static JournaledOrder slotReleased(int stockSlot) {
        return new JournaledOrder(EventType.SLOT_RELEASED, 0L, 0L, null, null, null, 0, null, 0L, 0L, 0L, stockSlot);
    }

    /** 구간 측정 시각 필드를 지정하지 않는 기존 호출부용 생성자 — 세 필드는 0이다. */
    public JournaledOrder(
        EventType type, long orderId, long accountId, String stockCode,
        OrderSide side, BigDecimal price, int quantity, Instant orderAt
    ) {
        this(type, orderId, accountId, stockCode, side, price, quantity, orderAt, 0L, 0L, 0L);
    }
}
