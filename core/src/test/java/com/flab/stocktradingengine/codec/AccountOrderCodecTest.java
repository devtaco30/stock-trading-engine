package com.flab.stocktradingengine.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Optional;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.trading.entity.OrderSide;

class AccountOrderCodecTest {

    private static final String STOCK = "005930";
    private final AccountOrderCodec codec = new AccountOrderCodec();

    @Test
    @DisplayName("매수 주문을 인코딩·디코딩하면 원래 필드가 그대로 복원된다")
    void 매수_라운드트립() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        DecodedAccountOrder order = new DecodedAccountOrder(
            OrderSide.BUY, 1L, STOCK, new BigDecimal("10000.50"), 10, "req-1", 1_700_000_000_123_456_789L);

        int length = codec.encode(buffer, 0, order);
        DecodedAccountOrder decoded = codec.decode(buffer, 0);

        assertEquals(OrderSide.BUY, decoded.type());
        assertEquals(1L, decoded.accountId());
        assertEquals(STOCK, decoded.stockCode());
        assertEquals(0, new BigDecimal("10000.50").compareTo(decoded.price()));
        assertEquals(10, decoded.quantity());
        assertEquals("req-1", decoded.requestId());
        assertEquals(1_700_000_000_123_456_789L, decoded.publishedAtEpochNanos());
        assertEquals(length, codec.encode(buffer, 0, decoded)); // 재인코딩 길이도 같아야 한다
    }

    @Test
    @DisplayName("매도 주문도 price가 그대로 왕복한다(②-a, 매칭 전달용)")
    void 매도_라운드트립_price도_왕복() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        DecodedAccountOrder order = new DecodedAccountOrder(
            OrderSide.SELL, 1L, STOCK, new BigDecimal("10000.50"), 4, "req-2", 1L);

        codec.encode(buffer, 0, order);
        DecodedAccountOrder decoded = codec.decode(buffer, 0);

        assertEquals(OrderSide.SELL, decoded.type());
        assertEquals(1L, decoded.accountId());
        assertEquals(STOCK, decoded.stockCode());
        assertEquals(0, new BigDecimal("10000.50").compareTo(decoded.price()));
        assertEquals(4, decoded.quantity());
        assertEquals("req-2", decoded.requestId());
    }

    @Test
    @DisplayName("0이 아닌 오프셋에서도 인코딩·디코딩이 맞는다")
    void 오프셋_있어도_라운드트립() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        DecodedAccountOrder order = new DecodedAccountOrder(
            OrderSide.BUY, 1L, STOCK, new BigDecimal("10000"), 10, "req-3", 2L);

        codec.encode(buffer, 13, order);
        DecodedAccountOrder decoded = codec.decode(buffer, 13);

        assertEquals(1L, decoded.accountId());
        assertEquals("req-3", decoded.requestId());
        assertEquals(2L, decoded.publishedAtEpochNanos());
    }

    @Test
    @DisplayName("정상 바이트는 tryDecode도 decode와 같은 값을 돌려준다")
    void tryDecode_정상바이트는_값을_돌려준다() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        DecodedAccountOrder order = new DecodedAccountOrder(
            OrderSide.BUY, 1L, STOCK, new BigDecimal("10000"), 10, "req-4", 3L);
        codec.encode(buffer, 0, order);

        Optional<DecodedAccountOrder> decoded = codec.tryDecode(buffer, 0);

        assertTrue(decoded.isPresent());
        assertEquals("req-4", decoded.get().requestId());
    }

    @Test
    @DisplayName("content-poison — type 바이트가 OrderSide 범위를 벗어나면 tryDecode는 예외 대신 빈 값을 돌려준다")
    void tryDecode_손상된_type바이트는_빈값() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        DecodedAccountOrder order = new DecodedAccountOrder(
            OrderSide.BUY, 1L, STOCK, new BigDecimal("10000"), 10, "req-5", 4L);
        codec.encode(buffer, 0, order);
        buffer.putByte(0, (byte) 99); // OrderSide.values().length(2)를 벗어난 값

        Optional<DecodedAccountOrder> decoded = codec.tryDecode(buffer, 0);

        assertFalse(decoded.isPresent());
    }
}
