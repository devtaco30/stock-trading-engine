package com.flab.stocktradingengine.account.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ConfigurableApplicationContext;

import com.flab.stocktradingengine.account.disruptor.AccountEngine;
import com.flab.stocktradingengine.account.disruptor.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.RejectReason;

/**
 * 설정값(account-worker.seed-accounts)으로 계좌를 시드한 {@link AccountEngine} 빈이
 * 컨텍스트 기동과 함께 실제로 도는지, 컨텍스트 종료 시 정상 셧다운되는지 확인한다.
 */
class AccountEngineConfigTest {

    private static final String STOCK = "005930";

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withUserConfiguration(AccountEngineConfig.class)
        .withPropertyValues(
            "account-worker.seed-accounts[0].account-id=1",
            "account-worker.seed-accounts[0].balance=1000000",
            "account-worker.seed-accounts[0].margin-rate=0.40"
        );

    @Test
    void 설정된_계좌를_시드하고_기동해_매수검증을_통과시킨다() throws InterruptedException {
        List<Recorded> events = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        contextRunner
            .withBean(AccountResultListener.class, () -> new Recorder(events, latch))
            .run(context -> {
                AccountEngine engine = context.getBean(AccountEngine.class);

                engine.publishBuy(1001L, 1L, STOCK, new BigDecimal("10000"), 10, "r1");
                assertThat(latch.await(1, TimeUnit.SECONDS)).as("1초 안에 결과가 도착해야 한다").isTrue();

                assertThat(events).hasSize(1);
                assertThat(events.get(0).accepted()).isTrue();

                assertShutsDownCleanly(context);
            });
    }

    private void assertShutsDownCleanly(ConfigurableApplicationContext context) {
        context.close(); // SmartLifecycle.stop() → engine.shutdown() 이 예외 없이 불려야 한다
    }

    private record Recorded(long accountId, long orderId, boolean accepted) {
    }

    private static final class Recorder implements AccountResultListener {
        private final List<Recorded> events;
        private final CountDownLatch latch;

        Recorder(List<Recorded> events, CountDownLatch latch) {
            this.events = events;
            this.latch = latch;
        }

        @Override
        public void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin) {
            events.add(new Recorded(accountId, orderId, true));
            latch.countDown();
        }

        @Override
        public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
            events.add(new Recorded(accountId, orderId, true));
            latch.countDown();
        }

        @Override
        public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
            events.add(new Recorded(accountId, orderId, false));
            latch.countDown();
        }

        @Override
        public void onFillApplied(long accountId, long orderId, long tradeId, boolean applied) {
            events.add(new Recorded(accountId, orderId, applied));
            latch.countDown();
        }
    }
}
