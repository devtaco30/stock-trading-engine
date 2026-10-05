package com.flab.stocktradingengine.account.disruptor.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicInteger;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.junit.jupiter.api.Test;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;

import com.flab.stocktradingengine.account.disruptor.domain.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.domain.RejectReason;
import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.support.IdleStrategies;

/** 폴링 스레드가 주입받은 쉬는 방식을 쓰고, 주입이 없으면 지금과 같은 기본 Backoff를 쓰는지 확인한다. */
class AccountOrderReceiverIdleStrategyTest {

    private static final String CHANNEL = "aeron:ipc";
    private static final int STREAM_ID = 3101;

    /** 호출 횟수만 세고 실제 쉬는 일은 기본 전략에 맡긴다. */
    private static final class CountingIdleStrategy implements IdleStrategy {
        final AtomicInteger idleCalls = new AtomicInteger();
        private final IdleStrategy delegate = new BackoffIdleStrategy();

        @Override
        public void idle(int workCount) {
            idleCalls.incrementAndGet();
            delegate.idle(workCount);
        }

        @Override
        public void idle() {
            idleCalls.incrementAndGet();
            delegate.idle();
        }

        @Override
        public void reset() {
            delegate.reset();
        }
    }

    private static final class NoopListener implements AccountResultListener {
        @Override
        public void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin) {
        }

        @Override
        public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
        }

        @Override
        public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
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
        }
    }

    private static AccountEngine engine() {
        AccountEngine engine = new AccountEngine(1024, 0L, (orderId, accountId, stockCode, side, price, quantity) -> {},
            new NoopListener());
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        return engine;
    }

    @Test
    void 주입한_쉬는_방식을_폴링_루프가_일이_없을_때_부른다() throws Exception {
        MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
            .aeronDirectoryName(CommonContext.generateRandomDirName()).dirDeleteOnStart(true).dirDeleteOnShutdown(true));
        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID);
        AccountEngine engine = engine();
        engine.start();
        CountingIdleStrategy counting = new CountingIdleStrategy();
        AccountOrderReceiver receiver = new AccountOrderReceiver(subscription, engine, counting);
        try {
            receiver.start();
            long deadline = System.nanoTime() + 2_000_000_000L;
            while (counting.idleCalls.get() == 0 && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }

            assertTrue(counting.idleCalls.get() > 0, "폴링 스레드가 주입한 전략의 idle을 불러야 한다");
        } finally {
            receiver.close();
            engine.shutdown();
            aeron.close();
            driver.close();
        }
    }

    @Test
    void 주입이_없으면_지금과_같은_기본_Backoff를_쓴다() throws Exception {
        assumeTrue(System.getenv(IdleStrategies.ENV_NAME) == null, "실험 모드가 꺼져 있을 때만 확인한다");
        AccountOrderReceiver receiver = new AccountOrderReceiver(null, null);

        Field field = AccountOrderReceiver.class.getDeclaredField("idleStrategy");
        field.setAccessible(true);

        assertEquals(BackoffIdleStrategy.class, field.get(receiver).getClass());
    }
}
