package com.flab.stocktradingengine.matching.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.StockSlotHasher;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.engine.OrderEvent;
import com.flab.stocktradingengine.matching.disruptor.handler.JournalEventHandler;
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventListener;
import com.flab.stocktradingengine.matching.disruptor.journal.InMemoryJournal;
import com.flab.stocktradingengine.matching.disruptor.journal.Journal;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.lmax.disruptor.BlockingWaitStrategy;

/**
 * 넘긴 쪽(X)의 정리 이벤트(U16b, 11-14절). 새 담당이 호가창을 확정한 뒤 맵이 바뀐 것을 본 X가 그 종목 slot의 호가창을 엔진 thread에서
 * 지운다. 저널에는 항목을 만들지 않고, 넘기는 중 목록은 다시 받을 때까지 둔다.
 */
class MatchingEngineSlotCleanupTest {

    private static final StockSlotHasher HASHER = new StockSlotHasher(64);
    private static final String STOCK = "005930";

    private final InMemoryJournal journal = new InMemoryJournal();
    private final List<Integer> cleanedSlots = new CopyOnWriteArrayList<>();
    private MatchingEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    private void startEngine() {
        engine = new MatchingEngine(1024, new BlockingWaitStrategy(), (stockCode, fill) -> { }, journal,
            HASHER, new SlotEventListener() {
                @Override
                public void onSlotCleanedUp(int stockSlot) {
                    cleanedSlots.add(stockSlot);
                }
            });
        engine.start();
    }

    private String stockInOtherSlot() {
        for (int code = 1; code < 1000; code++) {
            String candidate = String.format("%06d", code);
            if (HASHER.slotFor(candidate) != HASHER.slotFor(STOCK)) {
                return candidate;
            }
        }
        throw new IllegalStateException();
    }

    @Test
    void 정리_이벤트는_그_종목_slot의_호가창을_지우고_통지한다() throws Exception {
        startEngine();
        engine.publishPlace(1L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now());
        awaitContains(STOCK, 1L, true);

        engine.publishSlotCleanup(HASHER.slotFor(STOCK));
        awaitCleaned(1);

        assertFalse(engine.containsOrder(STOCK, 1L));
        assertEquals(List.of(HASHER.slotFor(STOCK)), cleanedSlots);
    }

    @Test
    void 다른_종목_slot의_호가창은_그대로다() throws Exception {
        startEngine();
        String other = stockInOtherSlot();
        engine.publishPlace(2L, 100L, other, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now());
        awaitContains(other, 2L, true);

        engine.publishSlotCleanup(HASHER.slotFor(STOCK));
        awaitCleaned(1);

        assertTrue(engine.containsOrder(other, 2L));
    }

    @Test
    void 정리_이벤트는_저널에_항목을_만들지_않는다() throws Exception {
        startEngine();

        engine.publishSlotCleanup(HASHER.slotFor(STOCK));
        awaitCleaned(1);

        assertTrue(journal.entries().isEmpty());
    }

    @Test
    void 정리해도_넘기는_중_목록은_그대로라_그_종목_slot의_주문은_계속_반영되지_않는다() throws Exception {
        startEngine();
        engine.publishSlotReleased(HASHER.slotFor(STOCK));
        engine.publishSlotCleanup(HASHER.slotFor(STOCK));
        engine.publishPlace(3L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now());
        String other = stockInOtherSlot();
        engine.publishPlace(4L, 100L, other, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now());
        awaitContains(other, 4L, true);

        assertFalse(engine.containsOrder(STOCK, 3L));
    }

    @Test
    void 저널_핸들러는_정리_이벤트_슬롯의_저널_위치를_저널의_현재_위치로_채운다() {
        FixedPositionJournal fixed = new FixedPositionJournal(2_048L);
        JournalEventHandler handler = new JournalEventHandler(fixed);
        OrderEvent slot = new OrderEvent();
        slot.setSlotCleanup(12);

        handler.onEvent(slot, 0L, true);

        assertEquals(2_048L, slot.getJournaledPosition());
        assertTrue(fixed.entries().isEmpty());
    }

    private void awaitCleaned(int count) throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (cleanedSlots.size() < count && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertTrue(cleanedSlots.size() >= count, "3초 안에 정리 통지가 나오지 않음");
    }

    private void awaitContains(String stockCode, long orderId, boolean expected) throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (engine.containsOrder(stockCode, orderId) != expected && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(expected, engine.containsOrder(stockCode, orderId));
    }

    private static final class FixedPositionJournal implements Journal {
        private final long position;
        private final List<JournaledOrder> appended = new CopyOnWriteArrayList<>();

        private FixedPositionJournal(long position) {
            this.position = position;
        }

        @Override
        public void append(JournaledOrder order) {
            appended.add(order);
        }

        @Override
        public List<JournaledOrder> entries() {
            return List.copyOf(appended);
        }

        @Override
        public long position() {
            return position;
        }
    }
}
