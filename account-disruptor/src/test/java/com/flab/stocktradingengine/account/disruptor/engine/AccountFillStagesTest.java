package com.flab.stocktradingengine.account.disruptor.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.account.disruptor.domain.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.domain.RejectReason;
import com.flab.stocktradingengine.time.EpochNanos;
import com.flab.stocktradingengine.time.StageLatencyRegistry;
import com.flab.stocktradingengine.time.StageSnapshot;

/**
 * 구간 분해 측정의 체결 반영 지점(W-P17~W-P20). 체결 한 건은 매수 계좌 쪽 이벤트에서만 E·F를 기록한다
 * (체결당 표본 한 건). 방출 시각이나 aggressor 발신 시각이 비어 있으면 기록하지 않는다.
 */
class AccountFillStagesTest {

    private static final String STOCK = "005930";
    private static final String[] FILL_STAGES = {
        "E", "F", "fill_transit", "fill_hop_to_journal", "fill_journal_append", "fill_hop_to_business"};

    private AccountEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    private AccountEngine newEngine(StageLatencyRegistry registry, CountDownLatch latch) {
        engine = new AccountEngine(1024, 0L, (orderId, accountId, stockCode, side, price, quantity) -> { },
            new FillListener(latch), registry);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.seed(2L, new BigDecimal("1000000"), new BigDecimal("0.40"), Map.of(STOCK, 10));
        engine.start();
        return engine;
    }

    private static long count(Map<String, StageSnapshot> snapshots, String stage) {
        StageSnapshot snapshot = snapshots.get(stage);
        return snapshot == null ? 0L : snapshot.count();
    }

    /** 예약 없이 체결부터 오면 도메인 불변식 위반으로 폐기되므로 먼저 매수·매도 예약을 잡는다. */
    private void reserve(AccountEngine account, CountDownLatch reserved) throws InterruptedException {
        account.publishBuy(1L, STOCK, new BigDecimal("10000"), 1, "rb");
        account.publishSell(2L, STOCK, new BigDecimal("10000"), 1, "rs");
        assertTrue(reserved.await(2, TimeUnit.SECONDS));
    }

    @Test
    void 매수_쪽_체결_반영에서_여섯_단계가_한_건씩_기록된다() throws InterruptedException {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        CountDownLatch reserved = new CountDownLatch(2);
        FillListener.reservedLatch = reserved;
        CountDownLatch applied = new CountDownLatch(2);
        AccountEngine account = newEngine(registry, applied);
        reserve(account, reserved);
        registry.snapshotAndResetAll();

        long origin = EpochNanos.now() - 100_000;
        long emitted = EpochNanos.now() - 10_000;
        long received = EpochNanos.now();
        long orderIdBuy = 1L;
        account.publishBuyFill(9001L, orderIdBuy, 1L, STOCK, new BigDecimal("10000"), 1, 0L, 0, received, emitted, origin);
        account.publishSellFill(9001L, 2L, 2L, STOCK, 1, 0L, 0, received, emitted, origin);
        assertTrue(applied.await(2, TimeUnit.SECONDS));

        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();
        for (String stage : FILL_STAGES) {
            assertEquals(1L, count(snapshots, stage), stage + " (매도 쪽까지 두 번 기록되면 2)");
        }
    }

    @Test
    void 방출_시각이_비어_있으면_기록하지_않는다() throws InterruptedException {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        CountDownLatch reserved = new CountDownLatch(2);
        FillListener.reservedLatch = reserved;
        CountDownLatch applied = new CountDownLatch(2);
        AccountEngine account = newEngine(registry, applied);
        reserve(account, reserved);
        registry.snapshotAndResetAll();

        account.publishBuyFill(9002L, 1L, 1L, STOCK, new BigDecimal("10000"), 1, 0L, 0, EpochNanos.now(), 0L, 0L);
        account.publishSellFill(9002L, 2L, 2L, STOCK, 1, 0L, 0, EpochNanos.now(), 0L, 0L);
        assertTrue(applied.await(2, TimeUnit.SECONDS));

        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();
        for (String stage : FILL_STAGES) {
            assertEquals(0L, count(snapshots, stage), stage);
        }
    }

    @Test
    void 계측을_끄면_아무것도_기록하지_않는다() throws InterruptedException {
        StageLatencyRegistry registry = new StageLatencyRegistry(false);
        CountDownLatch reserved = new CountDownLatch(2);
        FillListener.reservedLatch = reserved;
        CountDownLatch applied = new CountDownLatch(2);
        AccountEngine account = newEngine(registry, applied);
        reserve(account, reserved);

        long now = EpochNanos.now();
        account.publishBuyFill(9003L, 1L, 1L, STOCK, new BigDecimal("10000"), 1, 0L, 0, now, now - 10, now - 20);
        account.publishSellFill(9003L, 2L, 2L, STOCK, 1, 0L, 0, now, now - 10, now - 20);
        assertTrue(applied.await(2, TimeUnit.SECONDS));

        for (String stage : FILL_STAGES) {
            assertEquals(0L, count(registry.snapshotAndResetAll(), stage), stage);
        }
    }

    /** 접수 콜백은 reservedLatch를, 체결 반영 콜백은 생성자에 받은 latch를 내린다. */
    private static final class FillListener implements AccountResultListener {
        static volatile CountDownLatch reservedLatch;
        private final CountDownLatch appliedLatch;

        FillListener(CountDownLatch appliedLatch) {
            this.appliedLatch = appliedLatch;
        }

        @Override
        public void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin) {
            reservedLatch.countDown();
        }

        @Override
        public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
            reservedLatch.countDown();
        }

        @Override
        public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
        }

        @Override
        public void onFillApplied(long accountId, long orderId, long tradeId, boolean applied) {
            appliedLatch.countDown();
        }

        @Override
        public void onSettlementApplied(long accountId, long settlementRef, boolean applied) {
        }

        @Override
        public void onUnpaidRecorded(long accountId, long tradeId, BigDecimal amount) {
        }

        @Override
        public void onDuplicateRequest(long accountId, String requestId) {
        }
    }
}
