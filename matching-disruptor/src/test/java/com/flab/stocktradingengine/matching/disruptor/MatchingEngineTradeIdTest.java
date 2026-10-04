package com.flab.stocktradingengine.matching.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.io.MatchListener;
import com.flab.stocktradingengine.matching.disruptor.journal.InMemoryJournal;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshot;
import com.flab.stocktradingengine.time.StageLatencyRegistry;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.matching.FillResult;
import com.lmax.disruptor.BlockingWaitStrategy;

/**
 * v2 체결번호 결정론화(ADR-023 갱신 2c) 검증. 같은 입력이면 엔진마다 같은 번호를 받고, 저널 리플레이와 스냅샷 복원이
 * 카운터를 이어 주는지 본다. 번호는 listener가 아니라 엔진이 발급한다.
 */
class MatchingEngineTradeIdTest {

    private static final String STOCK = "005930";
    private static final int BUFFER_SIZE = 1024;
    private static final long NODE_ID = 4L;

    private MatchingEngine first;
    private MatchingEngine second;

    /** 5인자 onFill로 받은 tradeId만 모은다. 2인자·4인자 호출은 tradeId가 없으므로 기록하지 않는다. */
    private static final class RecordingListener implements MatchListener {
        final List<Long> tradeIds = new CopyOnWriteArrayList<>();

        @Override
        public void onFill(String stockCode, FillResult fill) {
        }

        @Override
        public void onFill(String stockCode, FillResult fill, long tradeId, long emittedAtNanos, long aggressorOriginNanos) {
            tradeIds.add(tradeId);
        }
    }

    @AfterEach
    void tearDown() {
        if (first != null) {
            first.shutdown();
        }
        if (second != null) {
            second.shutdown();
        }
    }

    private static long expected(long nodeId, long counter) {
        return (nodeId << 53) | counter;
    }

    private static void publishPair(MatchingEngine engine, int pairIndex) {
        long buyOrderId = pairIndex * 2L + 1;
        long sellOrderId = pairIndex * 2L + 2;
        engine.publishPlace(buyOrderId, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now());
        engine.publishPlace(sellOrderId, 200L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 10, Instant.now());
    }

    private static void awaitFills(RecordingListener listener, int count) throws InterruptedException {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (listener.tradeIds.size() < count && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertTrue(listener.tradeIds.size() >= count, "체결 " + count + "건을 기다렸지만 " + listener.tradeIds.size() + "건");
    }

    @Test
    @DisplayName("같은 입력을 두 엔진에 넣으면 같은 tradeId 열을 받는다")
    void sameInputSameTradeIds() throws InterruptedException {
        RecordingListener firstListener = new RecordingListener();
        RecordingListener secondListener = new RecordingListener();
        first = new MatchingEngine(BUFFER_SIZE, new BlockingWaitStrategy(), firstListener, new InMemoryJournal(),
            new StageLatencyRegistry(false), NODE_ID);
        second = new MatchingEngine(BUFFER_SIZE, new BlockingWaitStrategy(), secondListener, new InMemoryJournal(),
            new StageLatencyRegistry(false), NODE_ID);
        first.start();
        second.start();

        for (int i = 0; i < 3; i++) {
            publishPair(first, i);
            publishPair(second, i);
        }
        awaitFills(firstListener, 3);
        awaitFills(secondListener, 3);

        assertEquals(List.of(expected(NODE_ID, 1), expected(NODE_ID, 2), expected(NODE_ID, 3)), firstListener.tradeIds);
        assertEquals(firstListener.tradeIds, secondListener.tradeIds);
    }

    @Test
    @DisplayName("저널을 리플레이한 엔진은 리스너 없이도 카운터가 전진해 이어서 같은 번호를 준다")
    void recoverAdvancesCounter() throws InterruptedException {
        InMemoryJournal journal = new InMemoryJournal();
        RecordingListener liveListener = new RecordingListener();
        first = new MatchingEngine(BUFFER_SIZE, new BlockingWaitStrategy(), liveListener, journal,
            new StageLatencyRegistry(false), NODE_ID);
        first.start();
        publishPair(first, 0);
        publishPair(first, 1);
        awaitFills(liveListener, 2);
        List<JournaledOrder> entries = journal.entries();

        RecordingListener recoveredListener = new RecordingListener();
        second = new MatchingEngine(BUFFER_SIZE, new BlockingWaitStrategy(), recoveredListener, new InMemoryJournal(),
            new StageLatencyRegistry(false), NODE_ID);
        second.recover(entries);
        second.start();
        assertEquals(List.of(), recoveredListener.tradeIds, "리플레이는 listener를 부르지 않는다");

        publishPair(first, 2);
        publishPair(second, 2);
        awaitFills(liveListener, 3);
        awaitFills(recoveredListener, 1);

        assertEquals(expected(NODE_ID, 3), liveListener.tradeIds.get(2));
        assertEquals(liveListener.tradeIds.get(2), recoveredListener.tradeIds.get(0));
    }

    @Test
    @DisplayName("스냅샷에 카운터가 들어가고 복원한 엔진은 그다음 번호부터 이어 발급한다")
    void snapshotCarriesCounter() throws InterruptedException {
        RecordingListener liveListener = new RecordingListener();
        first = new MatchingEngine(BUFFER_SIZE, new BlockingWaitStrategy(), liveListener, new InMemoryJournal(),
            new StageLatencyRegistry(false), NODE_ID);
        first.start();
        publishPair(first, 0);
        publishPair(first, 1);
        awaitFills(liveListener, 2);
        Thread.sleep(50);

        MatchingSnapshot snapshot = first.snapshot();
        assertEquals(2L, snapshot.tradeIdCounter());

        RecordingListener restoredListener = new RecordingListener();
        second = new MatchingEngine(BUFFER_SIZE, new BlockingWaitStrategy(), restoredListener, new InMemoryJournal(),
            new StageLatencyRegistry(false), NODE_ID);
        second.restore(snapshot);
        second.start();
        publishPair(second, 2);
        awaitFills(restoredListener, 1);

        assertEquals(expected(NODE_ID, 3), restoredListener.tradeIds.get(0));
    }

    @Test
    @DisplayName("nodeId를 주지 않는 기존 생성자는 nodeId 0으로 발급한다")
    void defaultConstructorUsesNodeZero() throws InterruptedException {
        RecordingListener listener = new RecordingListener();
        first = new MatchingEngine(BUFFER_SIZE, listener);
        first.start();

        publishPair(first, 0);
        awaitFills(listener, 1);

        assertEquals(expected(0, 1), listener.tradeIds.get(0));
    }
}
