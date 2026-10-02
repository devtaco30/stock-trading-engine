package com.flab.stocktradingengine.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * 구간 분해 측정용 시각 필드(originNanos, receivedAtNanos, acceptedAtNanos / emittedAtNanos,
 * aggressorOriginNanos)가 와이어를 왕복해도 보존되는지, 필드를 지정하지 않은 기존 생성자는 0을 넣는지 확인한다.
 */
class SegmentTimeFieldsCodecTest {

    private final OrderCodec orderCodec = new OrderCodec();
    private final FillCodec fillCodec = new FillCodec();
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);

    @Test
    void PLACE_주문의_세_시각_필드가_왕복한다() {
        JournaledOrder original = new JournaledOrder(
            EventType.PLACE, 42L, 100L, "005930", OrderSide.BUY,
            new BigDecimal("10000"), 30, Instant.parse("2026-10-02T00:00:00.123456789Z"),
            1_700_000_000_000_000_001L, 1_700_000_000_000_000_002L, 1_700_000_000_000_000_003L);

        orderCodec.encode(buffer, 0, original);
        JournaledOrder decoded = orderCodec.decode(buffer, 0);

        assertEquals(1_700_000_000_000_000_001L, decoded.originNanos());
        assertEquals(1_700_000_000_000_000_002L, decoded.receivedAtNanos());
        assertEquals(1_700_000_000_000_000_003L, decoded.acceptedAtNanos());
        assertEquals("005930", decoded.stockCode(), "시각 필드 뒤의 종목 코드도 보존돼야 한다");
        assertEquals(30, decoded.quantity());
    }

    @Test
    void 시각을_지정하지_않은_기존_생성자는_세_필드를_0으로_채운다() {
        JournaledOrder legacy = new JournaledOrder(
            EventType.PLACE, 1L, 1L, "000660", OrderSide.SELL,
            new BigDecimal("1"), 1, Instant.EPOCH);

        orderCodec.encode(buffer, 0, legacy);
        JournaledOrder decoded = orderCodec.decode(buffer, 0);

        assertEquals(0L, decoded.originNanos());
        assertEquals(0L, decoded.receivedAtNanos());
        assertEquals(0L, decoded.acceptedAtNanos());
    }

    @Test
    void CANCEL_주문은_시각_필드가_없고_0으로_디코딩된다() {
        JournaledOrder cancel = new JournaledOrder(
            EventType.CANCEL, 7L, 0L, "005930", null, null, 0, null);

        int length = orderCodec.encode(buffer, 0, cancel);
        JournaledOrder decoded = orderCodec.decode(buffer, 0);

        assertEquals(EventType.CANCEL, decoded.type());
        assertEquals("005930", decoded.stockCode());
        assertEquals(0L, decoded.originNanos());
        assertEquals(1 + 8 + 4 + "005930".length(), length, "CANCEL 길이는 기존과 같아야 한다");
    }

    @Test
    void PLACE_인코딩_길이가_기존보다_정확히_24바이트_늘었다() {
        JournaledOrder order = new JournaledOrder(
            EventType.PLACE, 1L, 1L, "005930", OrderSide.BUY,
            new BigDecimal("1"), 1, Instant.EPOCH);

        int length = orderCodec.encode(buffer, 0, order);

        int legacyLength = 1 + 8 + 8 + 1 + 8 + 4 + 4 + 8 + 4 + 4 + "005930".length();
        assertEquals(legacyLength + 24, length);
    }

    @Test
    void 체결의_두_시각_필드가_왕복한다() {
        FilledTrade original = new FilledTrade(
            42L, "005930", 100L, 200L, 300L, 400L, 30, new BigDecimal("10000"),
            1_700_000_000_000_000_010L, 1_700_000_000_000_000_020L);

        int length = fillCodec.encode(buffer, 0, original);
        FilledTrade decoded = fillCodec.decode(buffer, 0);

        assertTrue(length > 0);
        assertEquals(1_700_000_000_000_000_010L, decoded.emittedAtNanos());
        assertEquals(1_700_000_000_000_000_020L, decoded.aggressorOriginNanos());
        assertEquals("005930", decoded.stockCode());
        assertEquals(30, decoded.filledQuantity());
    }

    @Test
    void 시각을_지정하지_않은_기존_체결_생성자는_두_필드를_0으로_채운다() {
        FilledTrade legacy = new FilledTrade(
            1L, "005930", 1L, 2L, 3L, 4L, 1, new BigDecimal("1"));

        fillCodec.encode(buffer, 0, legacy);
        FilledTrade decoded = fillCodec.decode(buffer, 0);

        assertEquals(0L, decoded.emittedAtNanos());
        assertEquals(0L, decoded.aggressorOriginNanos());
    }

    @Test
    void 체결_인코딩_길이가_기존보다_정확히_16바이트_늘었다() {
        FilledTrade fill = new FilledTrade(
            1L, "005930", 1L, 2L, 3L, 4L, 1, new BigDecimal("1"));

        int length = fillCodec.encode(buffer, 0, fill);

        int legacyLength = 8 * 5 + 4 + 8 + 4 + 4 + "005930".length();
        assertEquals(legacyLength + 16, length);
    }
}
