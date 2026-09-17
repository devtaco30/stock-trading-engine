package com.flab.stocktradingengine.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 주문 결과 기록 코덱의 라운드트립·truncation 검증. */
class OrderResultCodecTest {

    private final OrderResultCodec codec = new OrderResultCodec();

    @Test
    @DisplayName("ACCEPTED 엔트리 — rejectReason은 null")
    void ACCEPTED_라운드트립() {
        OrderResultEntry entry = new OrderResultEntry(1L, 100L, "r1", OrderVerdict.ACCEPTED, null, 1_000L);

        OrderResultEntry decoded = roundTrip(entry);

        assertEquals(1L, decoded.accountId());
        assertEquals(100L, decoded.orderId());
        assertEquals("r1", decoded.requestId());
        assertEquals(OrderVerdict.ACCEPTED, decoded.verdict());
        assertNull(decoded.rejectReason());
        assertEquals(1_000L, decoded.epochMillis());
    }

    @Test
    @DisplayName("REJECTED 엔트리 — rejectReason이 실린다")
    void REJECTED_라운드트립_rejectReason_실림() {
        OrderResultEntry entry =
            new OrderResultEntry(2L, 0L, "r2", OrderVerdict.REJECTED, "INSUFFICIENT", 2_000L);

        OrderResultEntry decoded = roundTrip(entry);

        assertEquals(0L, decoded.orderId());
        assertEquals(OrderVerdict.REJECTED, decoded.verdict());
        assertEquals("INSUFFICIENT", decoded.rejectReason());
    }

    @Test
    @DisplayName("DUPLICATE 엔트리 — orderId=0, rejectReason=null")
    void DUPLICATE_라운드트립() {
        OrderResultEntry entry = new OrderResultEntry(3L, 0L, "r3", OrderVerdict.DUPLICATE, null, 3_000L);

        OrderResultEntry decoded = roundTrip(entry);

        assertEquals(0L, decoded.orderId());
        assertEquals(OrderVerdict.DUPLICATE, decoded.verdict());
        assertNull(decoded.rejectReason());
    }

    @Test
    @DisplayName("requestId가 null이어도 왕복한다")
    void requestId_null_라운드트립() {
        OrderResultEntry entry = new OrderResultEntry(4L, 0L, null, OrderVerdict.DUPLICATE, null, 4_000L);

        OrderResultEntry decoded = roundTrip(entry);

        assertNull(decoded.requestId());
    }

    @Test
    @DisplayName("requestId가 빈 문자열이어도 null과 구분되어 왕복한다")
    void requestId_빈문자열_라운드트립() {
        OrderResultEntry entry = new OrderResultEntry(5L, 0L, "", OrderVerdict.DUPLICATE, null, 5_000L);

        OrderResultEntry decoded = roundTrip(entry);

        assertEquals("", decoded.requestId());
    }

    @Test
    @DisplayName("정상 바이트는 tryDecode도 encode한 값을 그대로 돌려준다")
    void tryDecode_정상바이트는_값을_돌려준다() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        OrderResultEntry entry = new OrderResultEntry(6L, 60L, "r6", OrderVerdict.ACCEPTED, null, 6_000L);
        int length = codec.encode(buffer, 0, entry);

        Optional<OrderResultEntry> decoded = codec.tryDecode(buffer, 0, length);

        assertTrue(decoded.isPresent());
        assertEquals("r6", decoded.get().requestId());
    }

    @Test
    @DisplayName("content-poison — verdict 바이트가 범위를 벗어나면 tryDecode는 예외 대신 빈 값을 돌려준다")
    void tryDecode_손상된_verdict바이트는_빈값() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        OrderResultEntry entry = new OrderResultEntry(7L, 70L, "r7", OrderVerdict.ACCEPTED, null, 7_000L);
        int length = codec.encode(buffer, 0, entry);
        buffer.putByte(0, (byte) 99); // OrderVerdict.values().length(3)를 벗어난 값

        Optional<OrderResultEntry> decoded = codec.tryDecode(buffer, 0, length);

        assertFalse(decoded.isPresent());
    }

    @Test
    @DisplayName("잘린 버퍼 — 고정 필드만큼도 안 되는 length는 tryDecode가 빈 값을 돌려준다")
    void tryDecode_고정필드_미만_길이는_빈값() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        OrderResultEntry entry = new OrderResultEntry(8L, 80L, "r8", OrderVerdict.ACCEPTED, null, 8_000L);
        codec.encode(buffer, 0, entry);

        Optional<OrderResultEntry> decoded = codec.tryDecode(buffer, 0, 5);

        assertFalse(decoded.isPresent());
    }

    @Test
    @DisplayName("잘린 버퍼 — 가변 문자열이 length 경계를 넘으면 tryDecode가 빈 값을 돌려준다")
    void tryDecode_가변필드_잘리면_빈값() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        OrderResultEntry entry = new OrderResultEntry(9L, 90L, "requestId9", OrderVerdict.ACCEPTED, null, 9_000L);
        int fullLength = codec.encode(buffer, 0, entry);

        Optional<OrderResultEntry> decoded = codec.tryDecode(buffer, 0, fullLength - 3);

        assertFalse(decoded.isPresent());
    }

    @Test
    @DisplayName("0이 아닌 오프셋에서도 인코딩·디코딩이 맞는다")
    void 오프셋_있어도_라운드트립() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        OrderResultEntry entry = new OrderResultEntry(10L, 0L, "r10", OrderVerdict.DUPLICATE, null, 10_000L);

        int length = codec.encode(buffer, 13, entry);
        Optional<OrderResultEntry> decoded = codec.tryDecode(buffer, 13, length);

        assertTrue(decoded.isPresent());
        assertEquals(10L, decoded.get().accountId());
        assertEquals("r10", decoded.get().requestId());
    }

    private OrderResultEntry roundTrip(OrderResultEntry entry) {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        int length = codec.encode(buffer, 0, entry);
        Optional<OrderResultEntry> decoded = codec.tryDecode(buffer, 0, length);
        assertTrue(decoded.isPresent());
        assertEquals(length, codec.encode(buffer, 0, decoded.get())); // 재인코딩 길이도 같아야 한다
        return decoded.get();
    }
}
