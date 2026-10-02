package com.flab.stocktradingengine.account.disruptor.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.account.disruptor.domain.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.domain.RejectReason;
import com.flab.stocktradingengine.account.disruptor.io.MatchingOrderSender;
import com.flab.stocktradingengine.time.EpochNanos;
import com.flab.stocktradingengine.time.StageLatencyRegistry;
import com.flab.stocktradingengine.time.StageSnapshot;
import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * 구간 분해 측정의 account-worker 지점(W-P2~W-P8). 합성 주문을 엔진에 흘려 단계별 히스토그램에
 * 기록되는 모집단(승인된 주문만)과, 시각이 매칭 입력까지 단조 증가로 실리는지를 확인한다.
 */
class AccountSegmentStagesTest {

    private static final String STOCK = "005930";
    private static final String[] ACCEPT_STAGES = {
        "A", "B", "B_biz", "ring_publish", "hop_to_journal", "journal_append", "hop_to_business"};

    private AccountEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    private AccountEngine newEngine(StageLatencyRegistry registry, CapturingSender sender, CountDownLatch latch) {
        engine = new AccountEngine(1024, 0L, sender, new CountingListener(latch), registry);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();
        return engine;
    }

    private static long count(Map<String, StageSnapshot> snapshots, String stage) {
        StageSnapshot snapshot = snapshots.get(stage);
        return snapshot == null ? 0L : snapshot.count();
    }

    @Test
    void 승인된_주문은_일곱_단계에_한_건씩_기록된다() throws InterruptedException {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        CountDownLatch latch = new CountDownLatch(1);
        AccountEngine account = newEngine(registry, new CapturingSender(), latch);

        long origin = EpochNanos.now();
        account.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1", origin, EpochNanos.now());
        assertTrue(latch.await(2, TimeUnit.SECONDS));

        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();
        for (String stage : ACCEPT_STAGES) {
            assertEquals(1L, count(snapshots, stage), stage);
        }
    }

    @Test
    void 매칭_입력에_실리는_세_시각은_단조_증가한다() throws InterruptedException {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        CountDownLatch latch = new CountDownLatch(1);
        CapturingSender sender = new CapturingSender();
        AccountEngine account = newEngine(registry, sender, latch);

        long origin = EpochNanos.now();
        account.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1", origin, EpochNanos.now());
        assertTrue(latch.await(2, TimeUnit.SECONDS));
        awaitForwarded(sender, 1);

        long[] times = sender.times.get(0);
        assertEquals(origin, times[0], "originNanos는 발신 시각 그대로");
        assertTrue(times[0] > 0 && times[0] <= times[1], "origin <= received");
        assertTrue(times[1] <= times[2], "received <= accepted");
        assertFalse(sender.sixArgCalled, "계측이 켜지면 시각을 실은 호출을 쓴다");
    }

    @Test
    void 거부된_주문은_어느_단계에도_기록하지_않는다() throws InterruptedException {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        CountDownLatch latch = new CountDownLatch(1);
        AccountEngine account = newEngine(registry, new CapturingSender(), latch);

        account.publishBuy(99L, STOCK, new BigDecimal("10000"), 10, "r1", EpochNanos.now(), EpochNanos.now());
        assertTrue(latch.await(2, TimeUnit.SECONDS));

        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();
        for (String stage : ACCEPT_STAGES) {
            assertEquals(0L, count(snapshots, stage), stage);
        }
    }

    @Test
    void 중복_requestId의_둘째_주문은_기록하지_않는다() throws InterruptedException {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        CountDownLatch latch = new CountDownLatch(2);
        AccountEngine account = newEngine(registry, new CapturingSender(), latch);

        account.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1", EpochNanos.now(), EpochNanos.now());
        account.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1", EpochNanos.now(), EpochNanos.now());
        assertTrue(latch.await(2, TimeUnit.SECONDS));

        assertEquals(1L, count(registry.snapshotAndResetAll(), "A"));
    }

    @Test
    void 매도_승인도_같은_일곱_단계에_기록된다() throws InterruptedException {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        CountDownLatch latch = new CountDownLatch(1);
        engine = new AccountEngine(1024, 0L, new CapturingSender(), new CountingListener(latch), registry);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"), Map.of(STOCK, 100));
        engine.start();

        engine.publishSell(1L, STOCK, new BigDecimal("10000"), 10, "s1", EpochNanos.now(), EpochNanos.now());
        assertTrue(latch.await(2, TimeUnit.SECONDS));

        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();
        for (String stage : ACCEPT_STAGES) {
            assertEquals(1L, count(snapshots, stage), stage);
        }
    }

    @Test
    void 계측을_끄면_아무것도_기록하지_않고_시각_없는_호출을_쓴다() throws InterruptedException {
        StageLatencyRegistry registry = new StageLatencyRegistry(false);
        CountDownLatch latch = new CountDownLatch(1);
        CapturingSender sender = new CapturingSender();
        AccountEngine account = newEngine(registry, sender, latch);

        account.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1", EpochNanos.now(), 0L);
        assertTrue(latch.await(2, TimeUnit.SECONDS));
        awaitForwarded(sender, 1);

        for (String stage : ACCEPT_STAGES) {
            assertEquals(0L, count(registry.snapshotAndResetAll(), stage), stage);
        }
        assertTrue(sender.sixArgCalled, "끈 상태는 기존 여섯 인자 호출 그대로");
    }

    private static void awaitForwarded(CapturingSender sender, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (sender.forwardedCount() < expected && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(expected, sender.forwardedCount());
    }

    /** 어느 forwardPlace 오버로드가 불렸는지와 실린 시각을 기록한다. */
    private static final class CapturingSender implements MatchingOrderSender {
        final List<long[]> times = new CopyOnWriteArrayList<>();
        volatile boolean sixArgCalled;
        private volatile int forwarded;

        int forwardedCount() {
            return forwarded;
        }

        @Override
        public void forwardPlace(long orderId, long accountId, String stockCode, OrderSide side, BigDecimal price, int quantity) {
            sixArgCalled = true;
            forwarded++;
        }

        @Override
        public void forwardPlace(long orderId, long accountId, String stockCode, OrderSide side, BigDecimal price, int quantity,
                                 long originNanos, long receivedAtNanos, long acceptedAtNanos) {
            times.add(new long[] {originNanos, receivedAtNanos, acceptedAtNanos});
            forwarded++;
        }
    }

    private static final class CountingListener implements AccountResultListener {
        private final CountDownLatch latch;

        CountingListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin) {
            latch.countDown();
        }

        @Override
        public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
            latch.countDown();
        }

        @Override
        public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
            latch.countDown();
        }

        @Override
        public void onFillApplied(long accountId, long orderId, long tradeId, boolean applied) {
        }

        @Override
        public void onSettlementApplied(long accountId, long settlementRef, boolean applied) {
        }

        @Override
        public void onUnpaidRecorded(long accountId, long tradeId, BigDecimal amount) {
        }

        @Override
        public void onDuplicateRequest(long accountId, String requestId) {
            latch.countDown();
        }
    }
}
