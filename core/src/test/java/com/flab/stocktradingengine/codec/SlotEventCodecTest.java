package com.flab.stocktradingengine.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * 종목 slot 받음·넘김 저널 항목(11-12절)의 와이어 모양. 항목은 맨 앞 1바이트가 {@link EventType}의 순번이고,
 * 새 두 값은 enum 맨 끝에 붙어 순번 2·3을 받는다. 그래서 옛 저널의 PLACE(0)·CANCEL(1) 바이트가 그대로 읽혀야 한다.
 */
class SlotEventCodecTest {

    private final OrderCodec codec = new OrderCodec();
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);

    @Test
    void 받음_항목이_종목_slot_번호와_함께_왕복한다() {
        int length = codec.encode(buffer, 0, JournaledOrder.slotAcquired(17));

        JournaledOrder decoded = codec.decode(buffer, 0);

        assertEquals(Byte.BYTES + Integer.BYTES, length);
        assertEquals(EventType.SLOT_ACQUIRED, decoded.type());
        assertEquals(17, decoded.stockSlot());
    }

    @Test
    void 넘김_항목이_종목_slot_번호와_함께_왕복한다() {
        codec.encode(buffer, 0, JournaledOrder.slotReleased(63));

        JournaledOrder decoded = codec.decode(buffer, 0);

        assertEquals(EventType.SLOT_RELEASED, decoded.type());
        assertEquals(63, decoded.stockSlot());
    }

    @Test
    void 받음_넘김_항목은_tryDecode로도_읽힌다() {
        codec.encode(buffer, 0, JournaledOrder.slotAcquired(3));

        Optional<JournaledOrder> decoded = codec.tryDecode(buffer, 0);

        assertTrue(decoded.isPresent());
        assertEquals(EventType.SLOT_ACQUIRED, decoded.get().type());
    }

    @Test
    void 이_항목이_생기기_전에_기록된_PLACE_바이트가_그대로_읽힌다() {
        // OrderCodec 클래스 주석의 PLACE 배치를 손으로 적은 바이트열이다(인코더를 거치지 않는다).
        UnsafeBuffer legacy = new UnsafeBuffer(new byte[256]);
        int p = 0;
        legacy.putByte(p, (byte) 0);                 // type: PLACE (순번 0)
        p += Byte.BYTES;
        legacy.putLong(p, 42L);                      // orderId
        p += Long.BYTES;
        legacy.putLong(p, 100L);                     // accountId
        p += Long.BYTES;
        legacy.putByte(p, (byte) OrderSide.SELL.ordinal());
        p += Byte.BYTES;
        legacy.putLong(p, 10000L);                   // priceUnscaled
        p += Long.BYTES;
        legacy.putInt(p, 0);                         // priceScale
        p += Integer.BYTES;
        legacy.putInt(p, 30);                        // quantity
        p += Integer.BYTES;
        legacy.putLong(p, 1_700_000_000L);           // orderAtEpochSecond
        p += Long.BYTES;
        legacy.putInt(p, 5);                         // orderAtNano
        p += Integer.BYTES;
        p += 3 * Long.BYTES;                         // originNanos, receivedAtNanos, acceptedAtNanos (0)
        legacy.putStringAscii(p, "005930");

        JournaledOrder decoded = codec.tryDecode(legacy, 0).orElseThrow();

        assertEquals(EventType.PLACE, decoded.type());
        assertEquals(42L, decoded.orderId());
        assertEquals(OrderSide.SELL, decoded.side());
        assertEquals(0, new BigDecimal("10000").compareTo(decoded.price()));
        assertEquals(30, decoded.quantity());
        assertEquals(Instant.ofEpochSecond(1_700_000_000L, 5), decoded.orderAt());
        assertEquals("005930", decoded.stockCode());
    }

    @Test
    void 이_항목이_생기기_전에_기록된_CANCEL_바이트가_그대로_읽힌다() {
        UnsafeBuffer legacy = new UnsafeBuffer(new byte[64]);
        legacy.putByte(0, (byte) 1);                 // type: CANCEL (순번 1)
        legacy.putLong(1, 7L);                       // orderId
        legacy.putStringAscii(1 + Long.BYTES, "005930");

        JournaledOrder decoded = codec.tryDecode(legacy, 0).orElseThrow();

        assertEquals(EventType.CANCEL, decoded.type());
        assertEquals(7L, decoded.orderId());
        assertEquals("005930", decoded.stockCode());
    }

    @Test
    void 새_값_바깥의_순번은_여전히_손상으로_걸러진다() {
        buffer.putByte(0, (byte) EventType.values().length);

        assertTrue(codec.tryDecode(buffer, 0).isEmpty());
    }
}
