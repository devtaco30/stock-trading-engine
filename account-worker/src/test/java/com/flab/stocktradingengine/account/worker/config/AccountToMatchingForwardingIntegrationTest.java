package com.flab.stocktradingengine.account.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;

import com.flab.stocktradingengine.account.disruptor.domain.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.domain.RejectReason;
import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.worker.AccountWorkerApplication;
import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.codec.OrderCodec;
import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;

/**
 * account-worker 앱을 실제로 띄우고, 계좌가 accept한 매수·매도가 진짜 Aeron IPC로 매칭 인테이크
 * 스트림까지 발신되는지 확인하는 인프로세스 end-to-end 테스트(②-b). {@link MatchingOrderSenderConfig}가
 * 만든 Publication과 같은 채널·스트림으로 이 테스트가 Subscription을 열어 받는다 — 실제 두 프로세스
 * (account-worker↔matching-worker) 연결은 범위 밖이다(C5-4).
 *
 * <p>{@code @DirtiesContext} — 이 컨텍스트가 만든 임베디드 MediaDriver를 다음 테스트와 안 겹치게 한다.</p>
 */
@SpringBootTest(
    classes = AccountWorkerApplication.class,
    properties = {
        "account-worker.seed-accounts[0].account-id=1",
        "account-worker.seed-accounts[0].balance=1000000",
        "account-worker.seed-accounts[0].margin-rate=0.40",
        "account-worker.seed-accounts[1].account-id=2",
        "account-worker.seed-accounts[1].balance=1000000",
        "account-worker.seed-accounts[1].margin-rate=0.40",
        "account-worker.seed-accounts[1].holdings[005930]=10"
    }
)
@DirtiesContext
@Import(AccountToMatchingForwardingIntegrationTest.RecorderConfig.class)
class AccountToMatchingForwardingIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;

    private final OrderCodec codec = new OrderCodec();

    @Autowired
    private Aeron aeron;

    @Autowired
    private AccountEngine engine;

    @Autowired
    private Recorder recorder;

    @Test
    void 매수가_accept되면_매칭으로_JournaledOrder_PLACE가_발신된다() throws InterruptedException {
        Subscription subscription = aeron.addSubscription(
            MatchingOrderSenderConfig.DEFAULT_MATCHING_CHANNEL, AeronStreamIds.MATCHING_INTAKE);
        try {
            awaitSubscribed(subscription); // best-effort 발신이라, 구독 전에 accept가 오면 조용히 유실된다

            recorder.prepare(1);
            Instant before = Instant.now();
            engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
            assertThat(recorder.await()).as("매수 accept 콜백이 도착해야 한다").isTrue();
            Instant after = Instant.now();

            JournaledOrder forwarded = pollForOrder(subscription);

            assertThat(forwarded.type()).isEqualTo(EventType.PLACE);
            assertThat(forwarded.orderId()).isEqualTo(recorder.lastAcceptedOrderId());
            assertThat(forwarded.accountId()).isEqualTo(1L);
            assertThat(forwarded.stockCode()).isEqualTo(STOCK);
            assertThat(forwarded.side()).isEqualTo(OrderSide.BUY);
            assertThat(forwarded.price()).isEqualByComparingTo("10000");
            assertThat(forwarded.quantity()).isEqualTo(10);
            assertThat(forwarded.orderAt()).isBetween(before, after);
        } finally {
            subscription.close();
        }
    }

    @Test
    void 매도가_accept되면_매칭으로_JournaledOrder_PLACE가_발신된다() throws InterruptedException {
        Subscription subscription = aeron.addSubscription(
            MatchingOrderSenderConfig.DEFAULT_MATCHING_CHANNEL, AeronStreamIds.MATCHING_INTAKE);
        try {
            awaitSubscribed(subscription);

            recorder.prepare(1);
            engine.publishSell(2L, STOCK, new BigDecimal("20000"), 4, "r2");
            assertThat(recorder.await()).as("매도 accept 콜백이 도착해야 한다").isTrue();

            JournaledOrder forwarded = pollForOrder(subscription);

            assertThat(forwarded.type()).isEqualTo(EventType.PLACE);
            assertThat(forwarded.orderId()).isEqualTo(recorder.lastAcceptedOrderId());
            assertThat(forwarded.accountId()).isEqualTo(2L);
            assertThat(forwarded.stockCode()).isEqualTo(STOCK);
            assertThat(forwarded.side()).isEqualTo(OrderSide.SELL);
            assertThat(forwarded.price()).isEqualByComparingTo("20000");
            assertThat(forwarded.quantity()).isEqualTo(4);
        } finally {
            subscription.close();
        }
    }

    /** 매칭 발신 Publication이 이 Subscription과 연결될 때까지 기다린다. */
    private void awaitSubscribed(Subscription subscription) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (!subscription.isConnected()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 매칭 발신 Publication과 연결되지 않음");
            }
            Thread.yield();
        }
    }

    /** Subscription을 폴링해 도착한 주문 하나를 디코딩해 돌려준다. */
    private JournaledOrder pollForOrder(Subscription subscription) {
        AtomicReference<JournaledOrder> received = new AtomicReference<>();
        FragmentHandler handler = (buffer, offset, length, header) -> received.set(codec.decode(buffer, offset));

        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (received.get() == null) {
            int fragments = subscription.poll(handler, 10);
            if (fragments == 0) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("5초 안에 매칭 발신 주문을 수신하지 못함");
                }
                Thread.yield();
            }
        }
        return received.get();
    }

    static class RecorderConfig {
        // @Primary가 아니다 — CompositeAccountResultListener(AccountEngineConfig)가
        // 이 Recorder도 delegate로 포함해 fan-out 하므로, Composite 쪽이 유일한 @Primary여야 한다.
        @Bean
        Recorder recorder() {
            return new Recorder();
        }
    }

    /** 테스트가 accept 콜백을 기다리고 발급된 orderId를 꺼내 쓸 수 있게 돕는 {@link AccountResultListener}. */
    static class Recorder implements AccountResultListener {
        private volatile CountDownLatch latch;
        private volatile long lastAcceptedOrderId;

        void prepare(int expectedResults) {
            latch = new CountDownLatch(expectedResults);
        }

        boolean await() throws InterruptedException {
            return latch.await(20, TimeUnit.SECONDS); // 최초 컨텍스트 기동 지연 감안(B4와 동일 이유)
        }

        long lastAcceptedOrderId() {
            return lastAcceptedOrderId;
        }

        @Override
        public void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin) {
            lastAcceptedOrderId = orderId;
            latch.countDown();
        }

        @Override
        public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
            lastAcceptedOrderId = orderId;
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
        }
    }
}
