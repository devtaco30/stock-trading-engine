package com.flab.stocktradingengine.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;

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
            OrderSide.BUY, 1L, STOCK, new BigDecimal("10000.50"), 10, "req-1");

        int length = codec.encode(buffer, 0, order);
        DecodedAccountOrder decoded = codec.decode(buffer, 0);

        assertEquals(OrderSide.BUY, decoded.type());
        assertEquals(1L, decoded.accountId());
        assertEquals(STOCK, decoded.stockCode());
        assertEquals(0, new BigDecimal("10000.50").compareTo(decoded.price()));
        assertEquals(10, decoded.quantity());
        assertEquals("req-1", decoded.requestId());
        assertEquals(length, codec.encode(buffer, 0, decoded)); // 재인코딩 길이도 같아야 한다
    }

    @Test
    @DisplayName("매도 주문을 인코딩·디코딩하면 price는 null로 복원된다")
    void 매도_라운드트립_price는_null() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        DecodedAccountOrder order = new DecodedAccountOrder(
            OrderSide.SELL, 1L, STOCK, null, 4, "req-2");

        codec.encode(buffer, 0, order);
        DecodedAccountOrder decoded = codec.decode(buffer, 0);

        assertEquals(OrderSide.SELL, decoded.type());
        assertEquals(1L, decoded.accountId());
        assertEquals(STOCK, decoded.stockCode());
        assertNull(decoded.price());
        assertEquals(4, decoded.quantity());
        assertEquals("req-2", decoded.requestId());
    }

    @Test
    @DisplayName("0이 아닌 오프셋에서도 인코딩·디코딩이 맞는다")
    void 오프셋_있어도_라운드트립() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        DecodedAccountOrder order = new DecodedAccountOrder(
            OrderSide.BUY, 1L, STOCK, new BigDecimal("10000"), 10, "req-3");

        codec.encode(buffer, 13, order);
        DecodedAccountOrder decoded = codec.decode(buffer, 13);

        assertEquals(1L, decoded.accountId());
        assertEquals("req-3", decoded.requestId());
    }
}
