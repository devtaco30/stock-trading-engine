package com.flab.stocktradingengine.matching.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.StockSlotHasher;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventListener;
import com.flab.stocktradingengine.matching.disruptor.journal.InMemoryJournal;
import com.flab.stocktradingengine.matching.disruptor.journal.Journal;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.lmax.disruptor.BlockingWaitStrategy;

/**
 * 종목 slot 받음·넘김 이벤트와 저널(U13). 종목 slot을 담당으로 켜는 쪽(Kafka 배정 콜백)은 받음 이벤트가 저널에
 * 기록된 뒤에 불리는 {@code onSlotAcquiredJournaled}를 신호로 쓴다.
 */
class MatchingEngineSlotEventJournalTest {

    private static final StockSlotHasher HASHER = new StockSlotHasher(64);

    private MatchingEngine engine;

    @AfterEach
    void tearDown() {
        engine.shutdown();
    }

    @Test
    void 받음_이벤트는_저널에_기록된_뒤에야_기록_완료_통지가_나간다() throws Exception {
        GatedJournal journal = new GatedJournal();
        List<Integer> journaledNotices = new CopyOnWriteArrayList<>();
        AtomicInteger journalSizeAtNotice = new AtomicInteger(-1);
        CountDownLatch noticeLatch = new CountDownLatch(1);
        engine = new MatchingEngine(1024, new BlockingWaitStrategy(), (stockCode, fill) -> { }, journal, HASHER,
            new SlotEventListener() {
                @Override
                public void onSlotAcquiredJournaled(int stockSlot) {
                    journalSizeAtNotice.set(journal.entries().size());
                    journaledNotices.add(stockSlot);
                    noticeLatch.countDown();
                }
            });
        engine.start();

        engine.publishSlotAcquired(5);
        assertTrue(journal.appendEntered.await(2, TimeUnit.SECONDS), "저널 핸들러가 받음 이벤트를 쓰기 시작해야 한다");
        Thread.sleep(200);
        assertTrue(journaledNotices.isEmpty(), "저널 기록이 끝나기 전에는 기록 완료 통지가 나가면 안 된다");

        journal.openGate();

        assertTrue(noticeLatch.await(2, TimeUnit.SECONDS));
        assertEquals(List.of(5), journaledNotices);
        assertEquals(1, journalSizeAtNotice.get(), "통지가 나간 시점에 저널에는 받음 항목이 이미 있어야 한다");
        assertEquals(EventType.SLOT_ACQUIRED, journal.entries().get(0).type());
        assertEquals(5, journal.entries().get(0).stockSlot());
    }

    @Test
    void 넘김_이벤트도_저널에_기록되고_받음_통지는_나가지_않는다() throws Exception {
        InMemoryJournal journal = new InMemoryJournal();
        AtomicInteger acquiredNotices = new AtomicInteger();
        engine = new MatchingEngine(1024, new BlockingWaitStrategy(), (stockCode, fill) -> { }, journal, HASHER,
            new SlotEventListener() {
                @Override
                public void onSlotAcquiredJournaled(int stockSlot) {
                    acquiredNotices.incrementAndGet();
                }
            });
        engine.start();

        engine.publishSlotReleased(9);
        awaitJournalSize(journal, 1);

        assertEquals(EventType.SLOT_RELEASED, journal.entries().get(0).type());
        assertEquals(9, journal.entries().get(0).stockSlot());
        assertEquals(0, acquiredNotices.get());
    }

    @Test
    void 받음_이벤트와_주문은_링버퍼에_넣은_순서대로_저널에_기록된다() throws Exception {
        InMemoryJournal journal = new InMemoryJournal();
        engine = new MatchingEngine(1024, new BlockingWaitStrategy(), (stockCode, fill) -> { }, journal, HASHER,
            SlotEventListener.NO_OP);
        engine.start();

        engine.publishSlotAcquired(5);
        engine.publishPlace(1L, 100L, "005930", OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now());
        awaitJournalSize(journal, 2);

        assertEquals(EventType.SLOT_ACQUIRED, journal.entries().get(0).type());
        assertEquals(EventType.PLACE, journal.entries().get(1).type());
    }

    @Test
    void 복구가_저널의_받음_넘김_항목을_만나도_호가창을_건드리지_않는다() {
        engine = new MatchingEngine(1024, (stockCode, fill) -> { });

        engine.recover(List.of(
            JournaledOrder.slotAcquired(5),
            new JournaledOrder(EventType.PLACE, 1L, 100L, "005930", OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now()),
            JournaledOrder.slotReleased(5)));

        assertTrue(engine.containsOrder("005930", 1L), "주문 항목은 그대로 재적용된다");
        assertFalse(engine.containsOrder("005930", 0L));
    }

    private static void awaitJournalSize(Journal journal, int size) throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (journal.entries().size() < size && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertTrue(journal.entries().size() >= size, "3초 안에 저널에 " + size + "건이 기록되지 않음");
    }

    /** 받음 항목을 append하는 순간 문이 열릴 때까지 붙잡아 두는 저널. */
    private static final class GatedJournal implements Journal {
        private final InMemoryJournal delegate = new InMemoryJournal();
        private final CountDownLatch gate = new CountDownLatch(1);
        final CountDownLatch appendEntered = new CountDownLatch(1);

        void openGate() {
            gate.countDown();
        }

        @Override
        public void append(JournaledOrder order) {
            appendEntered.countDown();
            try {
                gate.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            delegate.append(order);
        }

        @Override
        public List<JournaledOrder> entries() {
            return delegate.entries();
        }

        @Override
        public long position() {
            return delegate.position();
        }
    }
}
