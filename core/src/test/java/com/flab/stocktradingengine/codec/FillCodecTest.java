package com.flab.stocktradingengine.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Optional;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Unit 1 — 체결(fill) 바이너리 코덱 라운드트립.
 *
 * <p>인코딩한 바이트를 그대로 디코딩했을 때 모든 필드가 보존되는지 확인한다. Aeron 전송은 여기서 다루지 않는다 —
 * 순수 코덱 검증이라 버퍼에 쓰고 같은 버퍼에서 읽는다.</p>
 */
class FillCodecTest {

    private final FillCodec codec = new FillCodec();
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);

    @Test
    void 체결이_전_필드_보존되어_왕복한다() {
        FilledTrade original = new FilledTrade(
            42L, "005930", 100L, 200L, 300L, 400L, 30, new BigDecimal("10000"));

        int length = codec.encode(buffer, 0, original);
        FilledTrade decoded = codec.decode(buffer, 0);

        assertTrue(length > 0, "인코딩 길이가 양수여야 한다");
        assertEquals(42L, decoded.tradeId());
        assertEquals("005930", decoded.stockCode());
        assertEquals(100L, decoded.buyOrderId());
        assertEquals(200L, decoded.buyAccountId());
        assertEquals(300L, decoded.sellOrderId());
        assertEquals(400L, decoded.sellAccountId());
        assertEquals(30, decoded.filledQuantity());
        assertEquals(0, original.matchPrice().compareTo(decoded.matchPrice()), "가격 값이 같아야 한다");
    }

    @Test
    void 소수_scale_가진_가격도_보존된다() {
        FilledTrade original = new FilledTrade(
            1L, "000660", 1L, 2L, 3L, 4L, 5, new BigDecimal("10000.50"));

        codec.encode(buffer, 0, original);
        FilledTrade decoded = codec.decode(buffer, 0);

        assertEquals(0, original.matchPrice().compareTo(decoded.matchPrice()), "값이 같아야 한다");
        assertEquals(2, decoded.matchPrice().scale(), "scale(소수 자릿수)도 보존돼야 한다");
    }

    @Test
    void 스케일이_큰_가격도_보존된다() {
        FilledTrade original = new FilledTrade(
            9L, "035720", 11L, 12L, 13L, 14L, 7, new BigDecimal("1234.56789012"));

        codec.encode(buffer, 0, original);
        FilledTrade decoded = codec.decode(buffer, 0);

        assertEquals(0, original.matchPrice().compareTo(decoded.matchPrice()));
        assertEquals(8, decoded.matchPrice().scale(), "큰 소수 자릿수도 보존돼야 한다");
    }

    @Test
    void encode_반환값은_실제_기록_바이트_수와_같다() {
        FilledTrade original = new FilledTrade(
            42L, "005930", 100L, 200L, 300L, 400L, 30, new BigDecimal("10000"));

        int length = codec.encode(buffer, 0, original);

        // 고정 필드: long 5개(tradeId·buyOrderId·buyAccountId·sellOrderId·sellAccountId)=40바이트
        //          + filledQuantity(int 4) + priceUnscaled(long 8) + priceScale(int 4) = 16바이트
        // 가변 필드: stockCode = 길이 헤더 4 + ASCII N
        int expectedFixed = 40 + 16;
        int expectedStockCode = Integer.BYTES + "005930".length();
        assertEquals(expectedFixed + expectedStockCode, length, "반환 길이가 실제 기록 바이트 수와 같아야 한다");
    }

    @Test
    void 오프셋을_주면_그_위치부터_왕복한다() {
        int offset = 13; // 버퍼 앞을 비워두고 중간부터 써도 동작해야 한다
        FilledTrade original = new FilledTrade(
            99L, "035420", 200L, 201L, 202L, 203L, 1, new BigDecimal("55000"));

        codec.encode(buffer, offset, original);
        FilledTrade decoded = codec.decode(buffer, offset);

        assertEquals(99L, decoded.tradeId());
        assertEquals("035420", decoded.stockCode());
        assertEquals(0, original.matchPrice().compareTo(decoded.matchPrice()));
    }

    @Test
    void 최소값_최대값_id도_왕복한다() {
        FilledTrade original = new FilledTrade(
            Long.MIN_VALUE, "005930", Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE, 0L,
            Integer.MAX_VALUE, new BigDecimal("1"));

        codec.encode(buffer, 0, original);
        FilledTrade decoded = codec.decode(buffer, 0);

        assertEquals(Long.MIN_VALUE, decoded.tradeId());
        assertEquals(Long.MAX_VALUE, decoded.buyOrderId());
        assertEquals(Long.MIN_VALUE, decoded.buyAccountId());
        assertEquals(Long.MAX_VALUE, decoded.sellOrderId());
        assertEquals(0L, decoded.sellAccountId());
        assertEquals(Integer.MAX_VALUE, decoded.filledQuantity());
    }

    @Test
    void filledQuantity가_0이어도_왕복한다() {
        FilledTrade original = new FilledTrade(
            1L, "005930", 1L, 2L, 3L, 4L, 0, new BigDecimal("100"));

        codec.encode(buffer, 0, original);
        FilledTrade decoded = codec.decode(buffer, 0);

        assertEquals(0, decoded.filledQuantity());
    }

    @Test
    void 정상_바이트는_tryDecode도_값을_돌려준다() {
        FilledTrade original = new FilledTrade(
            42L, "005930", 100L, 200L, 300L, 400L, 30, new BigDecimal("10000"));
        codec.encode(buffer, 0, original);

        Optional<FilledTrade> decoded = codec.tryDecode(buffer, 0);

        assertTrue(decoded.isPresent());
        assertEquals(42L, decoded.get().tradeId());
        assertEquals("005930", decoded.get().stockCode());
    }

    @Test
    void content_poison_잘린_버퍼는_tryDecode가_빈값() {
        FilledTrade original = new FilledTrade(
            42L, "005930", 100L, 200L, 300L, 400L, 30, new BigDecimal("10000"));
        codec.encode(buffer, 0, original);

        // stockCode 길이 필드에 실제 버퍼보다 큰 값을 심어 경계 초과를 유발한다
        UnsafeBuffer truncated = new UnsafeBuffer(new byte[8]);
        Optional<FilledTrade> decoded = codec.tryDecode(truncated, 0);

        assertFalse(decoded.isPresent(), "잘린 버퍼는 던지지 않고 빈 값이어야 한다");
    }

    @Test
    void content_poison_filledQuantity가_음수면_tryDecode가_빈값() {
        FilledTrade original = new FilledTrade(
            42L, "005930", 100L, 200L, 300L, 400L, 30, new BigDecimal("10000"));
        codec.encode(buffer, 0, original);
        // filledQuantity 오프셋 = 40(long 5개), 손상 값(음수) 주입
        buffer.putInt(40, -1);

        Optional<FilledTrade> decoded = codec.tryDecode(buffer, 0);

        assertFalse(decoded.isPresent(), "filledQuantity 음수는 명백한 손상이라 빈 값이어야 한다");
    }
}
