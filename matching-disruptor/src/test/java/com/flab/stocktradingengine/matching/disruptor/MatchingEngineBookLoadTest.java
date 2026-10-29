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
        // 재시도는 간격을 두고 발행되므로 수신 thread의 반복처럼 계속 폴링한다.
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (sink.offers.isEmpty() && System.nanoTime() < deadline) {
            engine.pollLoadCompletions();
            Thread.sleep(1);
        }
        awaitSnapshotOffer(1);
        sink.durable.set(sink.offers.get(0).appliedSeq());
        engine.pollLoadCompletions();

        assertEquals(List.of(TRANSFER_ID), durableNotices);
    }

    @Test
    void 스냅샷이_계속_거부되어도_재시도는_간격을_두고_발행되어_바쁘게_돌지_않는다() throws Exception {
        sink.acceptOffers.set(false);
        startEngine();
        engine.publishBookLoad(TRANSFER_ID, HASHER.slotFor(STOCK), List.of(sourceBookBytes()));
        awaitOfferAttempts(1);

        // 수신 thread가 반복마다 부르는 것처럼 300ms 동안 쉬지 않고 폴링한다.
        long end = System.nanoTime() + 300_000_000L;
        while (System.nanoTime() < end) {
            engine.pollLoadCompletions();
        }

        long attempts = sink.attempts.get();
        assertTrue(attempts >= 2, "한 번은 재시도해야 한다: " + attempts);
        assertTrue(attempts <= 60, "재시도 간격(10ms)이 없으면 수천 번 돈다. 300ms 동안 시도 횟수=" + attempts);
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
    void 담당이_켜진_뒤_도착한_호가창_적재는_호가창을_바꾸지_않고_ERROR를_남긴다() throws Exception {
        java.util.logging.Logger julLogger = java.util.logging.Logger.getLogger(
            com.flab.stocktradingengine.matching.disruptor.handler.MatchingEventHandler.class.getName());
        List<java.util.logging.LogRecord> errors = new CopyOnWriteArrayList<>();
        java.util.logging.Handler handler = new java.util.logging.Handler() {
            @Override
            public void publish(java.util.logging.LogRecord record) {
                if (record.getLevel().intValue() >= java.util.logging.Level.SEVERE.intValue()) {
                    errors.add(record);
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        julLogger.addHandler(handler);
        try {
            startEngine();
            int slot = HASHER.slotFor(STOCK);
            byte[] bookBytes = sourceBookBytes();
            engine.publishBookLoad(TRANSFER_ID, slot, List.of(bookBytes));
            engine.publishSlotAcquired(slot);
            // 담당이 켜진 뒤 이 shard가 접수한 주문: 적재된 매수 1(잔량 6)과 3주 체결된다.
            engine.publishPlace(3L, 300L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 3, Instant.now());
            awaitFillQuantity(3);

            // 같은 종목 slot에 낡은 호가창(매수 1의 잔량 6)이 한 번 더 도착한다.
            engine.publishBookLoad(TRANSFER_ID + 1, slot, List.of(bookBytes));
            engine.publishPlace(4L, 400L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 30, Instant.now());
            awaitFillQuantity(6);
            Thread.sleep(100);

            int filledAgainstBuyOne = fills.stream().filter(fill -> fill.buyOrderId() == 1L)
                .mapToInt(FillResult::filledQuantity).sum();
            assertEquals(6, filledAgainstBuyOne, "낡은 호가창이 덮어썼다면 매수 1의 잔량이 6으로 돌아가 총 9가 체결된다");
            assertFalse(errors.isEmpty(), "적용하지 않은 적재는 ERROR 로그를 남긴다");
            assertTrue(errors.get(0).getMessage().contains("종목 slot=" + slot));
        } finally {
            julLogger.removeHandler(handler);
        }
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
    void 종목_slot_32개_적재가_연달아_와도_강제_스냅샷은_모아서_몇_번만_찍고_거부되지_않으며_완료_알림은_확정_뒤에_나간다() throws Exception {
        QueueSimSink simSink = new QueueSimSink();
        List<long[]> noticeSnapshots = new CopyOnWriteArrayList<>();
        SlotEventListener listener = new SlotEventListener() {
            @Override
            public void onBooksLoadedDurable(long transferId, int stockSlot) {
                durableNotices.add(transferId);
                noticeSnapshots.add(new long[] {simSink.durable.get(), simSink.firstOfferedSeq.get()});
            }
        };
        engine = new MatchingEngine(1024, new BlockingWaitStrategy(), (stockCode, fill) -> fills.add(fill), journal,
            simSink, true, new com.flab.stocktradingengine.time.StageLatencyRegistry(false), HASHER, listener);
        engine.start();
        List<String> stocks = stocksInDistinctSlots(32);

        // 수신 thread의 반복처럼, 적재 요청을 발행하는 thread가 같은 반복에서 완료 확인도 한다.
        for (int i = 0; i < stocks.size(); i++) {
            engine.publishBookLoad(1_000L + i, HASHER.slotFor(stocks.get(i)), List.of(singleStockBytes(stocks.get(i), 100L + i)));
            engine.pollLoadCompletions();
        }
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (durableNotices.size() < stocks.size() && System.nanoTime() < deadline) {
            engine.pollLoadCompletions();
            Thread.sleep(1);
        }

        assertEquals(32, durableNotices.size(), "32개 적재 모두 완료가 알려진다");
        assertTrue(simSink.attempts.get() <= 8, "강제 스냅샷 시도가 종목 slot 수(32)보다 훨씬 적어야 한다: " + simSink.attempts.get());
        assertEquals(0, simSink.rejections.get(), "쓰기 큐(용량 4)가 넘치도록 연달아 찍으면 안 된다");
        for (long[] atNotice : noticeSnapshots) {
            assertTrue(atNotice[0] >= atNotice[1] && atNotice[1] > 0,
                "완료 알림은 스냅샷이 디스크에 확정된 뒤에만 나간다: 알림 시점 확정 위치=" + atNotice[0] + " 첫 시도 위치=" + atNotice[1]);
        }
    }

    private List<String> stocksInDistinctSlots(int count) {
        List<String> stocks = new ArrayList<>();
        java.util.Set<Integer> used = new java.util.HashSet<>();
        for (int code = 1; stocks.size() < count && code < 10_000; code++) {
            String candidate = String.format("%06d", code);
            if (used.add(HASHER.slotFor(candidate))) {
                stocks.add(candidate);
            }
        }
        return stocks;
    }

    /** 종목 하나에 미체결 매수 주문 하나가 든 호가창 바이트. */
    private byte[] singleStockBytes(String stock, long orderId) {
        com.flab.stocktradingengine.matching.disruptor.snapshot.RestingOrder resting =
            new com.flab.stocktradingengine.matching.disruptor.snapshot.RestingOrder(
                orderId, 100L, OrderSide.BUY, new BigDecimal("10000"), 10, System.currentTimeMillis(), 0, false);
        return codec.encode(new MatchingSnapshot(
            Map.of(stock, new com.flab.stocktradingengine.matching.disruptor.snapshot.BookSnapshot(List.of(resting), Map.of())), 0L));
    }

    /** 쓰기 큐(용량 4)와 느린 쓰기 thread(스냅샷 하나에 5ms)를 흉내 내는 sink. 큐가 차면 offer가 false다. */
    private static final class QueueSimSink implements MatchingSnapshotSink {
        final AtomicLong attempts = new AtomicLong();
        final AtomicLong rejections = new AtomicLong();
        final AtomicLong durable = new AtomicLong();
        final AtomicLong firstOfferedSeq = new AtomicLong();
        private final java.util.concurrent.LinkedBlockingQueue<Long> queue = new java.util.concurrent.LinkedBlockingQueue<>();

        QueueSimSink() {
            Thread writer = new Thread(() -> {
                try {
                    while (true) {
                        long appliedSeq = queue.take();
                        Thread.sleep(5);
                        durable.set(appliedSeq);
                    }
                } catch (InterruptedException e) {
                    // 시험 종료
                }
            }, "queue-sim-writer");
            writer.setDaemon(true);
            writer.start();
        }

        @Override
        public boolean offer(byte[] snapshotBytes, Map<Integer, Long> orderIntakePositions, long appliedSeq) {
            attempts.incrementAndGet();
            if (queue.size() >= 4) {
                rejections.incrementAndGet();
                return false;
            }
            firstOfferedSeq.compareAndSet(0L, appliedSeq);
            queue.add(appliedSeq);
            return true;
        }

        @Override
        public long durableSeq() {
            return durable.get();
        }
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

    private void awaitFillQuantity(int quantity) throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (fills.stream().mapToInt(FillResult::filledQuantity).sum() < quantity && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertTrue(fills.stream().mapToInt(FillResult::filledQuantity).sum() >= quantity, "3초 안에 체결 수량이 모이지 않음");
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
