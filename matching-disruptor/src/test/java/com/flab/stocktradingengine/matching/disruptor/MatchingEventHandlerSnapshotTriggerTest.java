package com.flab.stocktradingengine.matching.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.matching.disruptor.engine.OrderEvent;
import com.flab.stocktradingengine.matching.disruptor.handler.MatchingEventHandler;
import com.flab.stocktradingengine.matching.disruptor.io.MatchingSnapshotSink;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshot;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshotCodec;
import com.flab.stocktradingengine.trading.matching.OrderBook;

/**
 * I6 U1 — 저널 적용 순번(appliedSeq)이 N에 도달하면 매칭 소비자 핸들러가 {@link
 * MatchingSnapshotSink}에 스냅샷을 정확히 한 번 offer하는지 검증한다. N(=10_000, {@code
 * MatchingEventHandler.SNAPSHOT_INTERVAL_JOURNAL_ENTRIES})을 그대로 쓴다 — account-disruptor
 * {@code AccountEngineSnapshotTriggerTest}와 같은 결. 이벤트는 존재하지 않는 주문의 취소(부작용
 * 없음)로 채워 appliedSeq만 밀어 올린다.
 */
class MatchingEventHandlerSnapshotTriggerTest {

    private static final String STOCK = "005930";
    private static final long SNAPSHOT_INTERVAL = 10_000L;

    @Test
    @DisplayName("저널 적용 순번이 N에 도달하면 싱크에 스냅샷을 정확히 한 번 offer한다")
    void N건마다_스냅샷을_한번_offer한다() {
        RecordingSnapshotSink sink = new RecordingSnapshotSink();
        MatchingEventHandler handler = newHandler(sink, true);

        publishDummyCancel(handler, SNAPSHOT_INTERVAL);

        assertEquals(1, sink.offers.size(), "정확히 N건째에 한 번만 offer돼야 한다");
        MatchingSnapshotCodec codec = new MatchingSnapshotCodec();
        MatchingSnapshot decoded = codec.decode(sink.offers.get(0).snapshotBytes());
        assertTrue(decoded.booksByStock().isEmpty(), "빈 호가창도 정상적으로 인코딩·디코딩돼야 한다");
        assertEquals(SNAPSHOT_INTERVAL, sink.offers.get(0).appliedSeq());
    }

    @Test
    @DisplayName("N-1건까지는 싱크에 offer하지 않는다")
    void N_1건까지는_offer_안_한다() {
        RecordingSnapshotSink sink = new RecordingSnapshotSink();
        MatchingEventHandler handler = newHandler(sink, true);

        publishDummyCancel(handler, SNAPSHOT_INTERVAL - 1);

        assertTrue(sink.offers.isEmpty(), "N-1건까지는 offer가 없어야 한다");
    }

    @Test
    @DisplayName("snapshotTriggerEnabled=false면 N건 경계를 넘겨도 offer하지 않는다 (복구 replay 전용 경로)")
    void snapshotTriggerEnabled_false면_offer_안_한다() {
        RecordingSnapshotSink sink = new RecordingSnapshotSink();
        MatchingEventHandler handler = newHandler(sink, false);

        publishDummyCancel(handler, SNAPSHOT_INTERVAL * 2);

        assertTrue(sink.offers.isEmpty(), "트리거를 꺼두면 경계를 두 번 넘어도 offer가 없어야 한다");
    }

    private static MatchingEventHandler newHandler(MatchingSnapshotSink sink, boolean triggerEnabled) {
        Map<String, OrderBook> books = new HashMap<>();
        return new MatchingEventHandler(books, (stockCode, fill) -> { }, sink, triggerEnabled);
    }

    /** 존재하지 않는 주문의 취소 — handleCancel이 book==null로 즉시 반환해 부작용 없이 appliedSeq만 오른다. */
    private static void publishDummyCancel(MatchingEventHandler handler, long count) {
        OrderEvent scratch = new OrderEvent();
        for (long i = 0; i < count; i++) {
            scratch.setCancel(999L, STOCK);
            handler.onEvent(scratch, i, false);
        }
    }

    /** offer된 스냅샷을 그대로 기록하는 테스트용 싱크. */
    private static final class RecordingSnapshotSink implements MatchingSnapshotSink {
        private final List<Offer> offers = new CopyOnWriteArrayList<>();

        @Override
        public boolean offer(byte[] snapshotBytes, long appliedSeq) {
            offers.add(new Offer(snapshotBytes, appliedSeq));
            return true;
        }

        @Override
        public long durableSeq() {
            return 0L;
        }

        private record Offer(byte[] snapshotBytes, long appliedSeq) {
        }
    }
}
