package com.flab.stocktradingengine.account.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.codec.OrderCodec;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;

/**
 * I2(발신측) U3 — 이 트랙 전체의 증명. account-worker 앱을 실제로 띄우되, 계좌가 주문을 접수할
 * 때는 매칭 인테이크 Subscription을 **아직 열지 않는다** — 매칭이 아직 그 주문을 받을 수 없는
 * 상태를 흉내낸다. {@link MatchingOrderSenderConfig} 빈이 만든 {@code Publication}은 이때 Aeron
 * {@code NOT_CONNECTED}로 계속 실패하고, {@code AeronMatchingOrderSender}의 전용 발신 스레드가
 * (D2) 조용히 계속 재시도한다 — 계좌 접수 자체(accept 콜백)는 이 재시도와 무관하게 즉시 끝난다
 * (재시도는 별도 스레드에서 돈다, 계좌 소비자 스레드는 큐잉만 하고 바로 돌아간다).
 *
 * <p>그 뒤에야 이 테스트가 Subscription을 연다 — "매칭이 뒤늦게 받기 시작한다"는 상황이다.
 * 재시도 중이던 발신이 그 즉시 성공해 주문이 도착해야 한다. 관측 신호는 새 카운터가 아니라
 * "도착 자체"다 — {@link #pollForOrder}가 {@code AccountToMatchingForwardingIntegrationTest}와
 * 똑같이 poll로 확인한다.</p>
 *
 * <p>U2b 전 코드(발신 큐가 가득 차거나 offer가 실패하면 로그만 남기고 버리던 상태)로 되돌리면,
 * 재시도 자체가 없어 이 주문은 영영 도착하지 않는다 — 그게 이 트랙의 RED다.</p>
 *
 * <p>{@code @DirtiesContext} — 이 컨텍스트가 만든 임베디드 MediaDriver를 다음 테스트와 안 겹치게 한다.</p>
 */
@SpringBootTest(
    classes = AccountWorkerApplication.class,
    properties = {
        "account-worker.seed-accounts[0].account-id=1",
        "account-worker.seed-accounts[0].balance=1000000",
        "account-worker.seed-accounts[0].margin-rate=0.40"
    }
)
@DirtiesContext
@Import(AccountOrderForwardingRetryIntegrationTest.RecorderConfig.class)
class AccountOrderForwardingRetryIntegrationTest {

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
    void 매칭이_아직_받지_못하는_상태에서_접수돼도_나중에_구독이_열리면_결국_도착한다() throws InterruptedException {
        recorder.prepare(1);
        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
        assertThat(recorder.await()).as("발신이 아직 안 됐어도 accept 콜백은 즉시 와야 한다").isTrue();

        // 이 시점에 매칭 인테이크 Publication엔 구독자가 없다 — 전용 발신 스레드가 실제 Aeron
        // NOT_CONNECTED로 조용히 재시도하는 중이다(D2). 이제서야 "매칭이 받기 시작한다"를 흉내내
        // 구독을 연다.
        Subscription subscription = aeron.addSubscription(
            MatchingOrderSenderConfig.DEFAULT_MATCHING_CHANNEL, AeronStreamIds.MATCHING_INTAKE);
        try {
            JournaledOrder forwarded = pollForOrder(subscription);

            assertThat(forwarded.orderId()).isEqualTo(recorder.lastAcceptedOrderId());
            assertThat(forwarded.accountId()).isEqualTo(1L);
            assertThat(forwarded.stockCode()).isEqualTo(STOCK);
        } finally {
            subscription.close();
        }
    }

    /** Subscription을 폴링해 도착한 주문 하나를 디코딩해 돌려준다. */
    private JournaledOrder pollForOrder(Subscription subscription) {
        JournaledOrder[] received = new JournaledOrder[1];
        FragmentHandler handler = (buffer, offset, length, header) -> received[0] = codec.decode(buffer, offset);

        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (received[0] == null) {
            int fragments = subscription.poll(handler, 10);
            if (fragments == 0) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("5초 안에 매칭 발신 주문을 수신하지 못함 — 재시도가 도착으로 안 이어졌다");
                }
                Thread.yield();
            }
        }
        return received[0];
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
            return latch.await(20, TimeUnit.SECONDS); // 최초 컨텍스트 기동 지연 감안
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
