package com.flab.stocktradingengine.matching.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

import com.lmax.disruptor.BlockingWaitStrategy;

import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.io.MatchListener;
import com.flab.stocktradingengine.matching.disruptor.journal.InMemoryJournal;
import com.flab.stocktradingengine.time.EpochNanos;
import com.flab.stocktradingengine.time.StageLatencyRegistry;
import com.flab.stocktradingengine.time.StageSnapshot;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.matching.FillResult;

/**
 * 구간 분해 측정의 매칭 지점(W-P11~W-P14). 주문 한 쌍을 흘려 C(매칭 입력 도착)는 접수된 주문마다,
 * D와 aggressor 기준 A·B·C는 체결을 일으킨 주문의 첫 체결에서만 기록되는지, 체결 이벤트에 방출 시각과
 * aggressor 발신 시각이 실리는지 확인한다.
 */
class MatchingSegmentStagesTest {

    private static final String STOCK = "005930";

    private MatchingEngine engine;
    private final List<long[]> fillTimes = new CopyOnWriteArrayList<>();
    private volatile boolean twoArgCalled;
    private CountDownLatch fillLatch = new CountDownLatch(1);

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    private MatchingEngine start(StageLatencyRegistry registry) {
        MatchListener listener = new MatchListener() {
            @Override
            public void onFill(String stockCode, FillResult fill) {
                twoArgCalled = true;
                fillLatch.countDown();
            }

            @Override
            public void onFill(String stockCode, FillResult fill, long emittedAtNanos, long aggressorOriginNanos) {
                fillTimes.add(new long[] {emittedAtNanos, aggressorOriginNanos});
                fillLatch.countDown();
            }
        };
        engine = new MatchingEngine(1024, new BlockingWaitStrategy(), listener, new InMemoryJournal(), registry);
        engine.start();
        return engine;
    }

    private static long count(Map<String, StageSnapshot> snapshots, String stage) {
        StageSnapshot snapshot = snapshots.get(stage);
        return snapshot == null ? 0L : snapshot.count();
    }

    private void placeWithTimes(long orderId, long accountId, OrderSide side, long origin) {
        long received = origin + 1_000;
        long accepted = received + 1_000;
        long matchReceived = EpochNanos.now();
        engine.publishPlace(orderId, accountId, STOCK, side, new BigDecimal("10000"), 1, Instant.now(),
            0L, 0, origin, received, accepted, matchReceived);
    }

    @Test
    void 체결이_나면_aggressor_기준_네_단계가_한_건씩_기록된다() throws InterruptedException {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        start(registry);
        long originSell = EpochNanos.now() - 50_000;
        long originBuy = EpochNanos.now() - 40_000;

        placeWithTimes(1L, 100L, OrderSide.SELL, originSell);
        placeWithTimes(2L, 200L, OrderSide.BUY, originBuy);
        assertTrue(fillLatch.await(2, TimeUnit.SECONDS));

        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();
        assertEquals(2L, count(snapshots, "C"), "접수된 주문마다 C");
        assertEquals(2L, count(snapshots, "m_ring_publish"));
        assertEquals(2L, count(snapshots, "m_hop_to_business"));
        for (String stage : new String[] {"D", "A@aggr", "B@aggr", "C@aggr"}) {
            assertEquals(1L, count(snapshots, stage), stage);
        }
        assertEquals(1, fillTimes.size());
        assertTrue(fillTimes.get(0)[0] > 0, "방출 시각이 실려야 한다");
        assertEquals(originBuy, fillTimes.get(0)[1], "aggressor는 나중에 도착한 매수 주문");
    }

    @Test
    void 체결이_없는_주문은_C까지만_기록한다() throws InterruptedException {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        start(registry);

        placeWithTimes(1L, 100L, OrderSide.SELL, EpochNanos.now() - 10_000);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (count(registry.snapshotAndResetAll(), "C") == 0 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        placeWithTimes(3L, 300L, OrderSide.SELL, EpochNanos.now() - 10_000);
        Thread.sleep(100);

        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();
        for (String stage : new String[] {"D", "A@aggr", "B@aggr", "C@aggr"}) {
            assertEquals(0L, count(snapshots, stage), stage);
        }
        assertTrue(fillTimes.isEmpty());
    }

    @Test
    void 계측을_끄면_아무것도_기록하지_않고_기존_두_인자_호출을_쓴다() throws InterruptedException {
        StageLatencyRegistry registry = new StageLatencyRegistry(false);
        start(registry);

        engine.publishPlace(1L, 100L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 1, Instant.now());
        engine.publishPlace(2L, 200L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 1, Instant.now());
        assertTrue(fillLatch.await(2, TimeUnit.SECONDS));

        for (String stage : new String[] {"C", "D", "A@aggr", "m_ring_publish"}) {
            assertEquals(0L, count(registry.snapshotAndResetAll(), stage), stage);
        }
        assertTrue(twoArgCalled);
        assertFalse(!fillTimes.isEmpty());
    }
}
