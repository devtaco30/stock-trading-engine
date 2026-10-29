package com.flab.stocktradingengine.matching.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.StockSlotHasher;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.io.MatchingSnapshotSink;
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventListener;
import com.flab.stocktradingengine.matching.disruptor.journal.InMemoryJournal;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshot;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshotCodec;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.matching.FillResult;
import com.lmax.disruptor.BlockingWaitStrategy;

/**
 * 「호가창 적재」 이벤트(U15). 호가창 이동을 받는 쪽(Z)이 받은 종목별 바이트를 엔진 thread에서 호가창에 적재하고,
 * 강제로 찍은 스냅샷이 디스크에 확정된 뒤에만(durableSeq가 그 스냅샷의 appliedSeq 이상) 완료를 알린다.
 */
class MatchingEngineBookLoadTest {

    private static final StockSlotHasher HASHER = new StockSlotHasher(64);
    private static final String STOCK = "005930";
    private static final long TRANSFER_ID = (6L << 53) | 3L;

    private final MatchingSnapshotCodec codec = new MatchingSnapshotCodec();
    private final List<FillResult> fills = new CopyOnWriteArrayList<>();
    private final List<Long> durableNotices = new CopyOnWriteArrayList<>();
    private final RecordingSink sink = new RecordingSink();
    private final InMemoryJournal journal = new InMemoryJournal();
    private MatchingEngine engine;

    @AfterEach
    void tearDown() {
        engine.shutdown();
    }

    private void startEngine() {
        SlotEventListener listener = new SlotEventListener() {
            @Override
            public void onBooksLoadedDurable(long transferId, int stockSlot) {
                durableNotices.add(transferId);
            }
        };
        engine = new MatchingEngine(1024, new BlockingWaitStrategy(), (stockCode, fill) -> fills.add(fill), journal,
            sink, true, new com.flab.stocktradingengine.time.StageLatencyRegistry(false), HASHER, listener);
        engine.start();
    }

    /** X의 호가창 하나를 찍어 종목 하나의 바이트로 만든다. */
    private byte[] sourceBookBytes() throws Exception {
        MatchingEngine source = new MatchingEngine(1024, (stockCode, fill) -> { });
        source.start();
        try {
            Instant now = Instant.now();
            source.publishPlace(1L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, now);
            source.publishPlace(2L, 200L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 4, now.plusMillis(1));
            long deadline = System.nanoTime() + 3_000_000_000L;
            while (!source.containsOrder(STOCK, 2L) && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            Thread.sleep(50);
        } finally {
            source.shutdown();
        }
        return codec.encode(source.snapshot());
    }

    @Test
    void 적재_이벤트는_받은_호가창을_되살려_이후_주문과_체결된다() throws Exception {
        startEngine();

        engine.publishBookLoad(TRANSFER_ID, HASHER.slotFor(STOCK), List.of(sourceBookBytes()));
        engine.publishPlace(3L, 300L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 6, Instant.now());
        awaitFill();

        assertEquals(1L, fills.get(0).buyOrderId(), "적재된 매수 주문 1(잔량 6)이 새 매도와 체결된다");
        assertTrue(engine.containsOrder(STOCK, 2L), "전량 체결된 주문의 체결 시각 기록도 적재된다");
    }

    @Test
    void 적재_이벤트는_저널에_기록하지_않는다() throws Exception {
        startEngine();

        engine.publishBookLoad(TRANSFER_ID, HASHER.slotFor(STOCK), List.of(sourceBookBytes()));
        awaitSnapshotOffer(1);

        assertTrue(journal.entries().isEmpty());
    }

    @Test
    void 적재_뒤_강제로_찍은_스냅샷에_적재한_호가창이_들어_있다() throws Exception {
        startEngine();

        engine.publishBookLoad(TRANSFER_ID, HASHER.slotFor(STOCK), List.of(sourceBookBytes()));
        awaitSnapshotOffer(1);

        MatchingSnapshot snapshot = codec.decode(sink.offers.get(0).bytes());
        assertTrue(snapshot.booksByStock().containsKey(STOCK));
    }

    @Test
    void 스냅샷이_디스크에_확정되기_전에는_완료_알림이_나가지_않는다() throws Exception {
        startEngine();
        engine.publishBookLoad(TRANSFER_ID, HASHER.slotFor(STOCK), List.of(sourceBookBytes()));
        awaitSnapshotOffer(1);

        sink.durable.set(sink.offers.get(0).appliedSeq() - 1);
        engine.pollLoadCompletions();

        assertTrue(durableNotices.isEmpty());
    }

    @Test
    void 스냅샷이_디스크에_확정된_뒤에_완료_알림이_한_번_나간다() throws Exception {
        startEngine();
        engine.publishBookLoad(TRANSFER_ID, HASHER.slotFor(STOCK), List.of(sourceBookBytes()));
        awaitSnapshotOffer(1);

        sink.durable.set(sink.offers.get(0).appliedSeq());
        engine.pollLoadCompletions();
        engine.pollLoadCompletions();

        assertEquals(List.of(TRANSFER_ID), durableNotices);
    }

    @Test
    void 쓰기_queue가_차서_스냅샷이_거부되면_재시도_이벤트로_다시_찍는다() throws Exception {
        sink.acceptOffers.set(false);
        startEngine();
        engine.publishBookLoad(TRANSFER_ID, HASHER.slotFor(STOCK), List.of(sourceBookBytes()));
        awaitOfferAttempts(1);
        engine.pollLoadCompletions();
        assertTrue(durableNotices.isEmpty());

        sink.acceptOffers.set(true);
        engine.pollLoadCompletions(); // 거부된 적재가 남아 있으면 강제 스냅샷 재시도 이벤트를 발행한다
        awaitSnapshotOffer(1);
        sink.durable.set(sink.offers.get(0).appliedSeq());
        engine.pollLoadCompletions();

        assertEquals(List.of(TRANSFER_ID), durableNotices);
    }

    @Test
    void 이미_호가창이_있는_종목_slot에_적재해도_주문이_두_번_쌓이지_않는다() throws Exception {
        startEngine();
        // 이 shard에 같은 종목의 호가창이 이미 있다(매수 1, 잔량 10). 적재하는 호가창에서는 매수 1의 잔량이 6이다.
        engine.publishPlace(1L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now());

        engine.publishBookLoad(TRANSFER_ID, HASHER.slotFor(STOCK), List.of(sourceBookBytes()));
        engine.publishPlace(3L, 300L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 30, Instant.now());
        awaitFill();
        Thread.sleep(100);

        int filledAgainstBuyOne = fills.stream().filter(fill -> fill.buyOrderId() == 1L)
            .mapToInt(FillResult::filledQuantity).sum();
        assertEquals(6, filledAgainstBuyOne, "기존 호가창을 비우고 적재했으면 잔량 6만 체결된다(두 번 쌓이면 16)");
    }

    @Test
    void 적재는_다른_종목_slot의_호가창을_비우지_않는다() throws Exception {
        startEngine();
        String otherSlotStock = stockInOtherSlot();
        engine.publishPlace(11L, 100L, otherSlotStock, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now());

        engine.publishBookLoad(TRANSFER_ID, HASHER.slotFor(STOCK), List.of(sourceBookBytes()));
        engine.publishPlace(12L, 300L, otherSlotStock, OrderSide.SELL, new BigDecimal("10000"), 10, Instant.now());
        awaitFill();

        assertEquals(11L, fills.get(0).buyOrderId());
    }

    private static String stockInOtherSlot() {
        for (int code = 1; code < 1000; code++) {
            String candidate = String.format("%06d", code);
            if (HASHER.slotFor(candidate) != HASHER.slotFor(STOCK)) {
                return candidate;
            }
        }
        throw new IllegalStateException("종목 slot이 다른 종목을 찾지 못했다");
    }

    @Test
    void 넘겼던_종목_slot을_다시_적재하면_그_종목의_주문이_다시_반영된다() throws Exception {
        startEngine();
        int slot = HASHER.slotFor(STOCK);
        engine.publishSlotReleased(slot);
        engine.publishPlace(7L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now());

        engine.publishBookLoad(TRANSFER_ID, slot, List.of(sourceBookBytes()));
        engine.publishPlace(8L, 300L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 6, Instant.now());
        awaitFill();

        assertFalse(engine.containsOrder(STOCK, 7L), "넘긴 뒤 적재 전에 온 주문은 반영되지 않았다");
        assertEquals(1L, fills.get(0).buyOrderId());
    }

    private void awaitFill() throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (fills.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertFalse(fills.isEmpty(), "3초 안에 체결이 나오지 않음");
    }

    private void awaitSnapshotOffer(int count) throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (sink.offers.size() < count && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertTrue(sink.offers.size() >= count, "3초 안에 스냅샷이 offer되지 않음");
    }

    private void awaitOfferAttempts(int count) throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (sink.attempts.get() < count && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertTrue(sink.attempts.get() >= count, "3초 안에 스냅샷 offer 시도가 없음");
    }

    private record Offer(byte[] bytes, long appliedSeq) {
    }

    private static final class RecordingSink implements MatchingSnapshotSink {
        final List<Offer> offers = new CopyOnWriteArrayList<>();
        final AtomicLong attempts = new AtomicLong();
        final AtomicLong durable = new AtomicLong();
        final AtomicBoolean acceptOffers = new AtomicBoolean(true);

        @Override
        public boolean offer(byte[] snapshotBytes, Map<Integer, Long> orderIntakePositions, long appliedSeq) {
            attempts.incrementAndGet();
            if (!acceptOffers.get()) {
                return false;
            }
            offers.add(new Offer(snapshotBytes, appliedSeq));
            return true;
        }

        @Override
        public long durableSeq() {
            return durable.get();
        }
    }
}
