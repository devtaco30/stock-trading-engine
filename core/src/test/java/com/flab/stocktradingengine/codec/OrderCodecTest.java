package com.flab.stocktradingengine.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * Unit 4b — 주문 바이너리 코덱 라운드트립.
 *
 * <p>인코딩한 바이트를 그대로 디코딩했을 때 모든 필드가 보존되는지 확인한다. Aeron 전송은 여기서 다루지 않는다 —
 * 순수 코덱 검증이라 버퍼에 쓰고 같은 버퍼에서 읽는다.</p>
 */
class OrderCodecTest {

    private final OrderCodec codec = new OrderCodec();
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);

    @Test
    void PLACE_주문이_전_필드_보존되어_왕복한다() {
        Instant orderAt = Instant.parse("2026-09-07T01:23:45.123456789Z");
        JournaledOrder original = new JournaledOrder(
            EventType.PLACE, 42L, 100L, "005930", OrderSide.BUY,
            new BigDecimal("10000"), 30, orderAt);

        int length = codec.encode(buffer, 0, original);
        JournaledOrder decoded = codec.decode(buffer, 0);

        assertTrue(length > 0, "인코딩 길이가 양수여야 한다");
        assertEquals(EventType.PLACE, decoded.type());
        assertEquals(42L, decoded.orderId());
        assertEquals(100L, decoded.accountId());
        assertEquals("005930", decoded.stockCode());
        assertEquals(OrderSide.BUY, decoded.side());
        assertEquals(0, original.price().compareTo(decoded.price()), "가격 값이 같아야 한다");
        assertEquals(30, decoded.quantity());
        assertEquals(orderAt, decoded.orderAt(), "나노초까지 보존돼야 한다");
    }

    @Test
    void 소수_scale_가진_가격도_보존된다() {
        JournaledOrder original = new JournaledOrder(
            EventType.PLACE, 1L, 1L, "000660", OrderSide.SELL,
            new BigDecimal("10000.50"), 5, Instant.EPOCH);

        codec.encode(buffer, 0, original);
        JournaledOrder decoded = codec.decode(buffer, 0);

        assertEquals(0, original.price().compareTo(decoded.price()), "값이 같아야 한다");
        assertEquals(2, decoded.price().scale(), "scale(소수 자릿수)도 보존돼야 한다");
    }

    @Test
    void CANCEL_주문은_orderId와_종목만_왕복한다() {
        JournaledOrder original = new JournaledOrder(
            EventType.CANCEL, 7L, 0L, "005930", null, null, 0, null);

        int length = codec.encode(buffer, 0, original);
        JournaledOrder decoded = codec.decode(buffer, 0);

        assertTrue(length > 0);
        assertEquals(EventType.CANCEL, decoded.type());
        assertEquals(7L, decoded.orderId());
        assertEquals("005930", decoded.stockCode());
        assertNull(decoded.side(), "CANCEL 은 방향이 없다");
        assertNull(decoded.price(), "CANCEL 은 가격이 없다");
        assertNull(decoded.orderAt(), "CANCEL 은 시각이 없다");
    }

    @Test
    void 오프셋을_주면_그_위치부터_왕복한다() {
        int offset = 13; // 버퍼 앞을 비워두고 중간부터 써도 동작해야 한다
        JournaledOrder original = new JournaledOrder(
            EventType.PLACE, 99L, 200L, "035420", OrderSide.SELL,
            new BigDecimal("55000"), 1, Instant.parse("2026-01-01T00:00:00Z"));

        codec.encode(buffer, offset, original);
        JournaledOrder decoded = codec.decode(buffer, offset);

        assertEquals(99L, decoded.orderId());
        assertEquals("035420", decoded.stockCode());
        assertEquals(0, original.price().compareTo(decoded.price()));
    }

    @Test
    void 정상_바이트는_tryDecode도_값을_돌려준다() {
        JournaledOrder original = new JournaledOrder(
            EventType.PLACE, 42L, 100L, "005930", OrderSide.BUY, new BigDecimal("10000"), 30, Instant.EPOCH);
        codec.encode(buffer, 0, original);

        Optional<JournaledOrder> decoded = codec.tryDecode(buffer, 0);

        assertTrue(decoded.isPresent());
        assertEquals(42L, decoded.get().orderId());
    }

    @Test
    void content_poison_type_바이트가_EventType_범위를_벗어나면_tryDecode는_빈값() {
        JournaledOrder original = new JournaledOrder(
            EventType.PLACE, 42L, 100L, "005930", OrderSide.BUY, new BigDecimal("10000"), 30, Instant.EPOCH);
        codec.encode(buffer, 0, original);
        buffer.putByte(0, (byte) 99); // EventType.values().length(2)를 벗어난 값

        Optional<JournaledOrder> decoded = codec.tryDecode(buffer, 0);

        assertFalse(decoded.isPresent());
    }
}
