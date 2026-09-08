package com.flab.stocktradingengine.account.worker;

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

import com.flab.stocktradingengine.account.disruptor.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.RejectReason;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.wire.AccountOrderCodec;
import com.flab.stocktradingengine.wire.DecodedAccountOrder;

import io.aeron.Aeron;
import io.aeron.Publication;

/**
 * account-worker 앱을 실제로 띄우고, 진짜 Aeron IPC로 매수 주문을 발신해 검증·예약까지 되는지
 * 확인하는 end-to-end 테스트(C5-1b). {@link AccountOrderIntakeConfig}가 만든 MediaDriver·Aeron을
 * 그대로 쓴다 — 같은 프로세스라 Aeron 빈을 재사용해 테스트용 Publication만 새로 연다.
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
@Import(AccountOrderIntakeIntegrationTest.RecorderConfig.class)
class AccountOrderIntakeIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;

    private final AccountOrderCodec codec = new AccountOrderCodec();

    @Autowired
    private Aeron aeron;

    @Autowired
    private Recorder recorder;

    @Test
    void Aeron_IPC로_받은_매수주문이_검증되고_예약된다() throws InterruptedException {
        recorder.prepare(1);
        Publication publication =
            aeron.addPublication(AccountOrderIntakeConfig.INTAKE_CHANNEL, AccountOrderIntakeConfig.INTAKE_STREAM_ID);
        try {
            awaitConnected(publication);
            send(publication, new DecodedAccountOrder(
                OrderSide.BUY, 1L, STOCK, new BigDecimal("10000"), 10, "r1"));

            assertThat(recorder.await()).as("5초 안에 결과가 도착해야 한다").isTrue();
            assertThat(recorder.rejections()).isEmpty();
        } finally {
            publication.close();
        }
    }

    /** 주문을 인코딩해 Aeron으로 발행한다. 백프레셔면 받아들여질 때까지 재시도한다. */
    private void send(Publication publication, DecodedAccountOrder order) {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        int length = codec.encode(buffer, 0, order);

        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        long result;
        do {
            result = publication.offer(buffer, 0, length);
            if (result > 0) {
                return;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 주문 발행 실패 — offer 반환=" + result);
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
        @Bean
        Recorder recorder() {
            return new Recorder();
        }
    }

    /** 테스트가 결과를 기다리고 검증할 수 있게 콜백을 모으는 {@link AccountResultListener}. */
    static class Recorder implements AccountResultListener {
        private final List<RejectReason> rejections = new CopyOnWriteArrayList<>();
        private volatile CountDownLatch latch;

        void prepare(int expectedResults) {
            latch = new CountDownLatch(expectedResults);
        }

        boolean await() throws InterruptedException {
            return latch.await(20, TimeUnit.SECONDS);
        }

        List<RejectReason> rejections() {
            return rejections;
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
            rejections.add(reason);
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
        public void onDuplicateRequest(long accountId, long orderId, String requestId) {
            latch.countDown();
        }
    }
}
