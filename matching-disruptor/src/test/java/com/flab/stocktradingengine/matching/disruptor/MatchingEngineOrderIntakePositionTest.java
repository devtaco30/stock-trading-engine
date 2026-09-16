package com.flab.stocktradingengine.matching.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.io.MatchListener;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.matching.FillResult;

/**
 * I2 U1 — 소비자가 실제로 처리한 주문의 인테이크 위치({@link MatchingEngine
 * #lastAppliedOrderIntakePositions()})는 아직 처리하지 않은 주문의 위치를 앞서 반영하면 안 된다.
 * account-disruptor {@code AccountEngineFillPositionTest}를 그대로 미러한다 — 발행자 하나만
 * 쓰므로 sessionId는 항상 0이다.
 */
class MatchingEngineOrderIntakePositionTest {

    private static final String STOCK = "005930";
    private static final int BUFFER_SIZE = 1024;

    private MatchingEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    @Test
    @DisplayName("2번째 주문까지만 소비자가 처리한 시점엔 lastAppliedOrderIntakePositions이 2번째 위치에 머물고, 3번째 위치를 앞서 반영하지 않는다")
    void 아직_처리_안_된_주문의_위치는_반영되지_않는다() throws InterruptedException {
        CountDownLatch reachedSecondFill = new CountDownLatch(1);
        CountDownLatch releaseSecondFill = new CountDownLatch(1);
        BlockingOnBuyOrderIdListener listener = new BlockingOnBuyOrderIdListener(2L, reachedSecondFill, releaseSecondFill);

        engine = new MatchingEngine(BUFFER_SIZE, listener);
        engine.start();

        Instant now = Instant.now();
        // 매도 하나를 먼저 채워 뒤이은 매수 세 건이 각각 1주씩 체결되게 한다.
        engine.publishPlace(999L, 200L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 3, now);

        engine.publishPlace(1L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 1, now.plusMillis(1), 100L, 0);
        engine.publishPlace(2L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 1, now.plusMillis(2), 200L, 0); // 여기서 리스너가 블록
        engine.publishPlace(3L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 1, now.plusMillis(3), 300L, 0); // 소비자 스레드가 아직 못 옴

        assertTrue(reachedSecondFill.await(1, TimeUnit.SECONDS), "2번째 주문(체결)까지는 처리돼야 한다");
        assertEquals(200L, engine.lastAppliedOrderIntakePositions().getOrDefault(0, 0L),
            "3번째 주문이 아직 처리 전이므로 lastAppliedOrderIntakePositions이 300을 앞서 가리키면 안 된다");

        releaseSecondFill.countDown();

        awaitPosition(300L);
        assertEquals(300L, engine.lastAppliedOrderIntakePositions().getOrDefault(0, 0L), "3번째 주문까지 처리되면 위치가 따라잡혀야 한다");
    }

    private void awaitPosition(long expected) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (engine.lastAppliedOrderIntakePositions().getOrDefault(0, 0L) < expected) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("1초 안에 lastAppliedOrderIntakePositions이 " + expected + "에 도달하지 않음");
            }
            Thread.onSpinWait();
        }
    }

    /** 지정한 buyOrderId의 체결을 만나면 release 래치가 열릴 때까지 소비자 스레드를 묶어 둔다. */
    private static final class BlockingOnBuyOrderIdListener implements MatchListener {
        private final long blockOnBuyOrderId;
        private final CountDownLatch reached;
        private final CountDownLatch release;

        BlockingOnBuyOrderIdListener(long blockOnBuyOrderId, CountDownLatch reached, CountDownLatch release) {
            this.blockOnBuyOrderId = blockOnBuyOrderId;
            this.reached = reached;
            this.release = release;
        }

        @Override
        public void onFill(String stockCode, FillResult fill) {
            if (fill.buyOrderId() == blockOnBuyOrderId) {
                reached.countDown();
                await(release);
            }
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
