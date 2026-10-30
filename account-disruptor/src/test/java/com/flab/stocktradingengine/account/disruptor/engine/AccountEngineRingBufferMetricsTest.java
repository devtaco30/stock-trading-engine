package com.flab.stocktradingengine.account.disruptor.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.account.disruptor.domain.NoOpAccountResultListener;

/** 12-3절 2번 표의 3단계: 계좌 ring buffer의 남은 칸 수를 읽을 수 있다. */
class AccountEngineRingBufferMetricsTest {

    private static final int BUFFER_SIZE = 1024;
    private static final long NODE_ID = 1L;

    @Test
    void 시작하기_전에는_알_수_없어_음수이고_시작한_뒤_처리할_주문이_없으면_전체_칸이_비어_있다() {
        AccountEngine engine = new AccountEngine(BUFFER_SIZE, NODE_ID,
            (orderId, accountId, stockCode, side, price, quantity) -> { }, NoOpAccountResultListener.INSTANCE);
        assertEquals(-1L, engine.ringBufferRemainingCapacity());

        engine.start();
        try {
            assertEquals((long) BUFFER_SIZE, engine.ringBufferRemainingCapacity());
        } finally {
            engine.shutdown();
        }
    }

    @Test
    void 빈_칸이_없어_발행이_기다린_횟수를_누적으로_센다() throws Exception {
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch forwarding = new java.util.concurrent.CountDownLatch(1);
        // 매칭행 전달이 막히면 계좌 엔진 thread가 멈춰 링버퍼가 찬다.
        AccountEngine engine = new AccountEngine(4, NODE_ID, (orderId, accountId, stockCode, side, price, quantity) -> {
            forwarding.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, NoOpAccountResultListener.INSTANCE);
        engine.seed(1L, new java.math.BigDecimal("100000000"), new java.math.BigDecimal("0.40"));
        engine.start();
        try {
            assertEquals(0L, engine.ringFullWaitCount());

            engine.publishBuy(1L, "005930", new java.math.BigDecimal("10000"), 1, "r0");
            assertTrue(forwarding.await(5, java.util.concurrent.TimeUnit.SECONDS), "엔진 thread가 전달에서 막혔다");
            for (int i = 1; i <= 3; i++) {
                engine.publishBuy(1L, "005930", new java.math.BigDecimal("10000"), 1, "r" + i);
            }
            assertEquals(0L, engine.ringFullWaitCount(), "칸이 남아 있는 동안은 기다리지 않는다");
            assertEquals(0L, engine.ringBufferRemainingCapacity(), "엔진 thread가 첫 건에서 막혀 네 건이 소비되지 않았고 링버퍼 네 칸이 가득 찼다");

            java.util.concurrent.CompletableFuture<Void> blockedPublish = java.util.concurrent.CompletableFuture.runAsync(
                () -> engine.publishBuy(1L, "005930", new java.math.BigDecimal("10000"), 1, "r4"));
            long deadline = System.currentTimeMillis() + 5_000L;
            while (engine.ringFullWaitCount() == 0L && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(1L, engine.ringFullWaitCount(), "빈 칸이 없어 기다린 발행 한 번을 센다");
            assertFalse(blockedPublish.isDone(), "발행은 칸이 날 때까지 기다린다");

            release.countDown();
            blockedPublish.get(5, java.util.concurrent.TimeUnit.SECONDS);
            assertEquals(1L, engine.ringFullWaitCount(), "기다린 뒤 성공한 발행도 한 번으로 센다");
        } finally {
            release.countDown();
            engine.shutdown();
        }
    }
}
