package com.flab.stocktradingengine.account.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.agrona.concurrent.UnsafeBuffer;
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
import com.flab.stocktradingengine.codec.FillCodec;
import com.flab.stocktradingengine.codec.FilledTrade;

import io.aeron.Aeron;
import io.aeron.Publication;

/**
 * account-worker 앱을 실제로 띄우고, 진짜 Aeron IPC로 체결을 발신해 계좌에 반영되는지 확인하는
 * end-to-end 테스트(ADR-032, U3). {@link AccountOrderIntakeIntegrationTest}와 같은 결(같은
 * 프로세스라 Aeron 빈을 재사용해 테스트용 Publication만 새로 연다) — matching-worker
 * {@code AccountFillPublisher}가 하는 일을 테스트가 직접 흉내낸다.
 *
 * <p>주문 접수 경로(Aeron)는 이 테스트 범위 밖이라, 체결이 도착하기 전에 필요한 매수·매도 예약은
 * 테스트가 {@link AccountEngine}에 직접 발행해 만든다 — "이미 예약된 주문에 체결이 도착한다"는
 * 전제를 흉내낸다(구 Kafka 버전 {@code AccountFillIntegrationTest}와 같은 전제).</p>
 *
 * <p>Kafka fan-out(같은 체결을 매수·매도 앞으로 두 번 발행)이 없어졌으므로 이 테스트도 한 번만
 * 발행한다 — 수신기가 그 한 건으로 매수·매도 양쪽을 다 반영한다.</p>
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
@Import(AccountFillIntegrationTest.RecorderConfig.class)
class AccountFillIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;

    private final FillCodec codec = new FillCodec();

    @Autowired
    private AccountEngine engine;

    @Autowired
    private Aeron aeron;

    @Autowired
    private Recorder recorder;

    @Test
    void 실제_Aeron으로_받은_체결을_매수_매도_양쪽에_반영한다() throws InterruptedException {
        recorder.prepare(2); // 매수 예약(1) + 매도 예약(1)
        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 4, "r1");
        engine.publishSell(2L, STOCK, new BigDecimal("10000"), 4, "r2");
        assertThat(recorder.await()).as("예약 콜백이 1초 안에 도착해야 한다").isTrue();
        assertThat(recorder.rejections()).as("매수·매도 예약이 전부 통과해야 한다").isEmpty();
        // 계좌 워커가 발급한 실제 orderId(C5-2a) — 클라이언트가 정하지 않으므로 accept 콜백에서 꺼내 쓴다.
        long buyOrderId = recorder.acceptedOrderIds().get(0);
        long sellOrderId = recorder.acceptedOrderIds().get(1);

        recorder.prepare(2); // 매수 체결반영(1) + 매도 체결반영(1) — fan-out이 없어 딱 2개만 온다
        FilledTrade trade = new FilledTrade(9001L, STOCK, buyOrderId, 1L, sellOrderId, 2L, 4, new BigDecimal("10000"));
        Publication publication = aeron.addPublication(AccountFillIntakeConfig.FILL_CHANNEL, AccountFillIntakeConfig.FILL_STREAM_ID);
        try {
            awaitConnected(publication);
            send(publication, trade);

            assertThat(recorder.await()).as("체결 반영 콜백 2개가 도착해야 한다").isTrue();
            assertThat(recorder.fillEvents()).containsExactlyInAnyOrder(
                new Recorder.FillEvent(1L, 9001L, true),
                new Recorder.FillEvent(2L, 9001L, true)
            );
        } finally {
            publication.close();
        }
    }

    /** 체결을 인코딩해 Aeron으로 발행한다. 백프레셔면 받아들여질 때까지 재시도한다. */
    private void send(Publication publication, FilledTrade trade) {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        int length = codec.encode(buffer, 0, trade);

        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        long result;
        do {
            result = publication.offer(buffer, 0, length);
            if (result > 0) {
                return;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 체결 발행 실패 — offer 반환=" + result);
            }
            Thread.yield();
        } while (true);
    }

    private void awaitConnected(Publication publication) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (!publication.isConnected()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 Publication이 연결되지 않음");
            }
            Thread.yield();
        }
    }

    static class RecorderConfig {
        // @Primary가 아니다 — CompositeAccountResultListener(AccountEngineConfig)가
        // 이 Recorder도 delegate로 포함해 fan-out 하므로, Composite 쪽이 유일한 @Primary여야 한다.
        @Bean
        Recorder recorder() {
            return new Recorder();
        }
    }

    /** 테스트가 결과를 기다리고 검증할 수 있게 콜백을 모으는 {@link AccountResultListener}. */
    static class Recorder implements AccountResultListener {
        private final List<FillEvent> events = new CopyOnWriteArrayList<>();
        private final List<RejectReason> rejections = new CopyOnWriteArrayList<>();
        private final List<Long> acceptedOrderIds = new CopyOnWriteArrayList<>();
        private volatile CountDownLatch latch;

        void prepare(int expectedResults) {
            latch = new CountDownLatch(expectedResults);
        }

        boolean await() throws InterruptedException {
            return latch.await(20, TimeUnit.SECONDS);
        }

        List<FillEvent> fillEvents() {
            return events;
        }

        List<RejectReason> rejections() {
            return rejections;
        }

        /** 계좌 워커가 매수·매도 접수 순서대로 발급한 orderId(C5-2a) — 클라이언트가 정하지 않아 fill 이벤트를 만들 때 여기서 꺼내 쓴다. */
        List<Long> acceptedOrderIds() {
            return acceptedOrderIds;
        }

        @Override
        public void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin) {
            acceptedOrderIds.add(orderId);
            latch.countDown();
        }

        @Override
        public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
            acceptedOrderIds.add(orderId);
            latch.countDown();
        }

        @Override
        public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
            rejections.add(reason);
            latch.countDown();
        }

        @Override
        public void onFillApplied(long accountId, long orderId, long tradeId, boolean applied) {
            events.add(new FillEvent(accountId, tradeId, applied));
            latch.countDown();
        }

        @Override
        public void onSettlementApplied(long accountId, long settlementRef, boolean applied) {
            latch.countDown();
        }

        @Override
        public void onUnpaidRecorded(long accountId, long tradeId, BigDecimal amount) {
        }

        @Override
        public void onDuplicateRequest(long accountId, long orderId, String requestId) {
        }

        record FillEvent(long accountId, long tradeId, boolean applied) {
        }
    }
}
