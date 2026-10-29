package com.flab.stocktradingengine.matching.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.journal.InMemoryJournal;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.matching.FillResult;
import com.lmax.disruptor.BlockingWaitStrategy;

/**
 * 거부한 주문의 위치만 담은 이벤트(11-4절 3항). 엔진 thread가 이 이벤트로 발행자별 반영 위치만 앞으로 나아가게
 * 하고, 호가창은 건드리지 않으며, 저널에는 기록하지 않는다.
 */
class MatchingEnginePositionOnlyTest {

    private static final String STOCK = "005930";

    private final List<FillResult> fills = new CopyOnWriteArrayList<>();
    private final CountDownLatch fillLatch = new CountDownLatch(1);
    private final InMemoryJournal journal = new InMemoryJournal();
    private MatchingEngine engine;

    @AfterEach
    void tearDown() {
        engine.shutdown();
    }

    private void startEngine() {
        engine = new MatchingEngine(1024, new BlockingWaitStrategy(), (stockCode, fill) -> {
            fills.add(fill);
            fillLatch.countDown();
        }, journal);
        engine.start();
    }

    @Test
    void 위치만_담은_이벤트는_발행자별_반영_위치를_앞으로_나아가게_한다() throws Exception {
        startEngine();

        engine.publishPositionOnly(7, 500L);
        awaitPosition(7, 500L);
        engine.publishPositionOnly(7, 900L);
        awaitPosition(7, 900L);

        assertEquals(Map.of(7, 900L), engine.lastAppliedOrderIntakePositions());
    }

    @Test
    void 위치만_담은_이벤트는_호가창을_건드리지_않는다() throws Exception {
        startEngine();
        Instant now = Instant.now();
        engine.publishPlace(1L, 100L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 100, now);
        engine.publishPositionOnly(7, 500L);
        awaitPosition(7, 500L);

        engine.publishPlace(2L, 200L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 100, now.plusMillis(1));

        assertTrue(fillLatch.await(2, TimeUnit.SECONDS), "앞서 호가창에 들어간 매도 주문이 그대로 있어 체결되어야 한다");
        assertEquals(1, fills.size());
        assertEquals(1L, fills.get(0).sellOrderId());
    }

    @Test
    void 위치만_담은_이벤트는_저널에_기록하지_않는다() throws Exception {
        startEngine();
        engine.publishPlace(1L, 100L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 100, Instant.now());
        engine.publishPositionOnly(7, 500L);
        awaitPosition(7, 500L);

        assertEquals(1, journal.entries().size(), "저널에는 주문 접수 하나만 있어야 한다");
    }

    private void awaitPosition(int sessionId, long position) throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (System.nanoTime() < deadline) {
            Long applied = engine.lastAppliedOrderIntakePositions().get(sessionId);
            if (applied != null && applied == position) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("3초 안에 위치가 반영되지 않음: " + engine.lastAppliedOrderIntakePositions());
    }
}
