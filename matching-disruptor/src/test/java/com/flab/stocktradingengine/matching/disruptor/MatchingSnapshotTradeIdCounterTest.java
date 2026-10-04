package com.flab.stocktradingengine.matching.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.matching.disruptor.snapshot.BookSnapshot;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshot;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshotCodec;
import com.flab.stocktradingengine.matching.disruptor.snapshot.RestingOrder;
import com.flab.stocktradingengine.trading.entity.OrderSide;

/** 스냅샷 코덱이 체결번호 카운터를 journalPosition 바로 뒤에 보존하고, 카운터가 없던 옛 배치도 읽는지 본다. */
class MatchingSnapshotTradeIdCounterTest {

    private final MatchingSnapshotCodec codec = new MatchingSnapshotCodec();

    private static MatchingSnapshot sample(long counter) {
        RestingOrder resting = new RestingOrder(1L, 100L, OrderSide.BUY, new BigDecimal("10000"), 10, 1_700_000_000_000L, 0, false);
        BookSnapshot book = new BookSnapshot(List.of(resting), Map.of(7L, 1_700_000_001_000L));
        return new MatchingSnapshot(Map.of("005930", book), 4242L, counter);
    }

    @Test
    @DisplayName("인코딩·디코딩 왕복에서 카운터가 보존된다")
    void counterRoundTrips() {
        MatchingSnapshot decoded = codec.decode(codec.encode(sample(123_456L)));

        assertEquals(123_456L, decoded.tradeIdCounter());
        assertEquals(4242L, decoded.journalPosition());
        assertEquals(sample(0L).booksByStock(), decoded.booksByStock());
    }

    @Test
    @DisplayName("카운터가 없던 옛 배치는 decodeLegacy로 읽고 카운터는 0이다")
    void legacyLayoutReadsWithZeroCounter() {
        byte[] current = codec.encode(sample(99L));
        // 옛 배치 = journalPosition:8 뒤의 카운터 8바이트를 뺀 것
        byte[] legacy = new byte[current.length - Long.BYTES];
        System.arraycopy(current, 0, legacy, 0, Long.BYTES);
        System.arraycopy(current, 2 * Long.BYTES, legacy, Long.BYTES, current.length - 2 * Long.BYTES);

        MatchingSnapshot decoded = codec.decodeLegacy(legacy);

        assertEquals(0L, decoded.tradeIdCounter());
        assertEquals(4242L, decoded.journalPosition());
        assertEquals(sample(0L).booksByStock(), decoded.booksByStock());
    }

    @Test
    @DisplayName("옛 배치를 새 디코더로 읽으면 같은 결과가 나오지 않는다(두 배치를 섞어 쓰지 않는다)")
    void legacyBytesAreNotReadableAsCurrent() {
        byte[] current = codec.encode(sample(99L));
        byte[] legacy = Arrays.copyOfRange(current, 0, current.length);
        System.arraycopy(current, 2 * Long.BYTES, legacy, Long.BYTES, current.length - 2 * Long.BYTES);
        byte[] shortened = Arrays.copyOf(legacy, current.length - Long.BYTES);

        boolean sameAsOriginal;
        try {
            sameAsOriginal = sample(0L).booksByStock().equals(codec.decode(shortened).booksByStock());
        } catch (RuntimeException e) {
            sameAsOriginal = false;
        }
        assertEquals(false, sameAsOriginal);
    }
}
