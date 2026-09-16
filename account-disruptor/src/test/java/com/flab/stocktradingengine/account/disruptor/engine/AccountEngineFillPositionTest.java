package com.flab.stocktradingengine.account.disruptor.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.account.disruptor.domain.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.domain.RejectReason;
import com.flab.stocktradingengine.account.disruptor.io.MatchingOrderSender;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * 1-2 — 소비자가 실제로 처리한 체결의 위치({@link AccountEngine#lastAppliedFillPositions()})는
 * 아직 처리하지 않은 체결의 위치를 앞서 반영하면 안 된다. 수신 스레드가 링에 넣은 시점의 위치를
 * 그대로 쓰면 소비자가 못 따라간 체결이 스냅샷 경계 안쪽으로 잘못 들어가 복구 때 빠진다 — 이
 * 테스트는 그 경계가 소비자 처리 시점에 정확히 멈춰서는지 본다. 이 테스트는 발행자 하나만
 * 쓰므로 sessionId는 항상 0이다(ADR-032 I1 D1) — 여러 발행자를 구분하는 검증은
 * {@code AccountFillReceiverTest}·{@code AccountFillReplayRecoveryIntegrationTest}(U3) 몫이다.
 */
class AccountEngineFillPositionTest {

    private static final String STOCK = "005930";
    private static final long NODE_ID = 0L;
    private static final MatchingOrderSender NO_OP_SENDER = (orderId, accountId, stockCode, side, price, quantity) -> {};

    private AccountEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    @Test
    @DisplayName("2번째 체결까지만 소비자가 처리한 시점엔 lastAppliedFillPosition이 2번째 위치에 머물고, 3번째 위치를 앞서 반영하지 않는다")
    void 아직_처리_안_된_체결의_위치는_반영되지_않는다() throws InterruptedException {
        CountDownLatch acceptLatch = new CountDownLatch(1);
        CountDownLatch reachedSecondFill = new CountDownLatch(1);
        CountDownLatch releaseSecondFill = new CountDownLatch(1);
        AtomicLong orderIdHolder = new AtomicLong();
        BlockingOnTradeIdListener listener =
            new BlockingOnTradeIdListener(9002L, acceptLatch, orderIdHolder, reachedSecondFill, releaseSecondFill);

        engine = new AccountEngine(1024, new BlockingWaitStrategy(), ProducerType.SINGLE, NODE_ID, NO_OP_SENDER, listener);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 3, "r1");
        assertTrue(acceptLatch.await(1, TimeUnit.SECONDS), "매수 예약이 accept돼야 한다");
        long orderId = orderIdHolder.get();

        engine.publishBuyFill(9001L, orderId, 1L, STOCK, new BigDecimal("10000"), 1, 100L);
        engine.publishBuyFill(9002L, orderId, 1L, STOCK, new BigDecimal("10000"), 1, 200L); // 여기서 리스너가 블록
        engine.publishBuyFill(9003L, orderId, 1L, STOCK, new BigDecimal("10000"), 1, 300L); // 소비자 스레드가 아직 못 옴

        assertTrue(reachedSecondFill.await(1, TimeUnit.SECONDS), "2번째 체결까지는 처리돼야 한다");
        assertEquals(200L, engine.lastAppliedFillPositions().getOrDefault(0, 0L),
            "3번째 체결이 아직 처리 전이므로 lastAppliedFillPositions이 300을 앞서 가리키면 안 된다");

        releaseSecondFill.countDown();

        awaitPosition(300L);
        assertEquals(300L, engine.lastAppliedFillPositions().getOrDefault(0, 0L), "3번째 체결까지 처리되면 위치가 따라잡혀야 한다");
    }

    private void awaitPosition(long expected) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (engine.lastAppliedFillPositions().getOrDefault(0, 0L) < expected) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("1초 안에 lastAppliedFillPositions이 " + expected + "에 도달하지 않음");
            }
            Thread.onSpinWait();
        }
    }

    /** accept에서 orderId를 캡처하고, onFillApplied가 지정한 tradeId를 만나면 release 래치가 열릴 때까지 소비자 스레드를 묶어 둔다. */
    private static final class BlockingOnTradeIdListener implements AccountResultListener {
        private final long blockOnTradeId;
        private final CountDownLatch accepted;
        private final AtomicLong orderIdHolder;
        private final CountDownLatch reached;
        private final CountDownLatch release;

        BlockingOnTradeIdListener(long blockOnTradeId, CountDownLatch accepted, AtomicLong orderIdHolder,
                CountDownLatch reached, CountDownLatch release) {
            this.blockOnTradeId = blockOnTradeId;
            this.accepted = accepted;
            this.orderIdHolder = orderIdHolder;
            this.reached = reached;
            this.release = release;
        }

        @Override
        public void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin) {
            orderIdHolder.set(orderId);
            accepted.countDown();
        }

        @Override
        public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
        }

        @Override
        public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
        }

        @Override
        public void onFillApplied(long accountId, long orderId, long tradeId, boolean applied) {
            if (tradeId == blockOnTradeId) {
                reached.countDown();
                await(release);
            }
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

        private static void await(CountDownLatch latch) {
            try {
                latch.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
