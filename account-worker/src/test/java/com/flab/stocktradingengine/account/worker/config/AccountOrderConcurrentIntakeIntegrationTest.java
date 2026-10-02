package com.flab.stocktradingengine.account.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import com.flab.stocktradingengine.account.disruptor.domain.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.domain.RejectReason;
import com.flab.stocktradingengine.account.worker.AccountWorkerApplication;
import com.flab.stocktradingengine.account.worker.support.NoOpMatchingOrderSenderTestConfig;
import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.codec.AccountOrderCodec;
import com.flab.stocktradingengine.codec.DecodedAccountOrder;
import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.Aeron;
import io.aeron.Publication;

/**
 * 같은 계좌에 여러 스레드가 동시에 매수 주문을 보내도, 잔고를 넘는 주문이 통과하지 않는지 확인하는
 * end-to-end 테스트(A3-a, IPC).
 *
 * <p>운영에서 동시성이 생기는 곳은 api 요청 스레드 여럿이 하나의 {@link Publication}에 동시에
 * {@code offer}하는 단계다. 계좌 워커 쪽은 폴링 스레드 하나가 받아 링버퍼에 넣고 업무 핸들러 하나가
 * 처리한다. 이 테스트는 api의 {@code AeronAccountOrderSender} 대신 {@link AccountOrderCodec}으로
 * 인코딩한 주문을 공유 Publication 하나에 T개 스레드가 동시에 {@code offer}하는 방식으로 그 단계를
 * 재현한다(account-worker 모듈은 api 모듈에 의존하지 않는다).</p>
 *
 * <p>컨텍스트 하나에서 반복하면 예약이 누적되므로, 반복마다 다른 계좌를 쓴다. 계좌는
 * {@link #seedAccounts}가 한꺼번에 시드한다. 결과는 엔진 업무 스레드에서 불리는 콜백으로만 모으고
 * {@code engine.accountState()}는 읽지 않는다.</p>
 */
@SpringBootTest(classes = AccountWorkerApplication.class)
@DirtiesContext
@Import({AccountOrderConcurrentIntakeIntegrationTest.RecorderConfig.class, NoOpMatchingOrderSenderTestConfig.class})
@Timeout(120)
class AccountOrderConcurrentIntakeIntegrationTest {

    private static final String STOCK = "005930";
    private static final String CHANNEL = "aeron:ipc";
    private static final long OFFER_TIMEOUT_NANOS = 5_000_000_000L;

    private static final int ORDER_COUNT = 60;
    private static final BigDecimal INITIAL_BALANCE = new BigDecimal("1000000");
    private static final BigDecimal ORDER_PRICE = new BigDecimal("10000");
    private static final int ORDER_QUANTITY = 10;

    private static final int REPEAT = 20;
    private static final long FIRST_ACCOUNT_THREADS_8 = 1L;
    private static final long FIRST_ACCOUNT_THREADS_16 = 101L;
    private static final long FIRST_ACCOUNT_MARGIN_040 = 201L;
    private static final long FIRST_ACCOUNT_SEQUENTIAL = 301L;

    @Autowired
    private Aeron aeron;

    @Autowired
    private Recorder recorder;

    @DynamicPropertySource
    static void seedAccounts(DynamicPropertyRegistry registry) {
        seedBlock(registry, 0, FIRST_ACCOUNT_THREADS_8, "1.00");
        seedBlock(registry, REPEAT, FIRST_ACCOUNT_THREADS_16, "1.00");
        seedBlock(registry, REPEAT * 2, FIRST_ACCOUNT_MARGIN_040, "0.40");
        seedBlock(registry, REPEAT * 3, FIRST_ACCOUNT_SEQUENTIAL, "1.00");
    }

    private static void seedBlock(DynamicPropertyRegistry registry, int firstIndex, long firstAccountId,
                                  String marginRate) {
        for (int offset = 0; offset < REPEAT; offset++) {
            int index = firstIndex + offset;
            String prefix = "account-worker.seed-accounts[" + index + "].";
            long accountId = firstAccountId + offset;
            registry.add(prefix + "account-id", () -> String.valueOf(accountId));
            registry.add(prefix + "balance", () -> INITIAL_BALANCE.toPlainString());
            registry.add(prefix + "margin-rate", () -> marginRate);
        }
    }

    @Test
    void 증거금률_1_00에서_8개_스레드가_동시에_보내도_통과는_10건이다() throws Exception {
        for (int round = 0; round < REPEAT; round++) {
            long accountId = FIRST_ACCOUNT_THREADS_8 + round;
            Outcome outcome = sendOrders(accountId, 8);
            assertOutcome(outcome, 10);
        }
    }

    @Test
    void 증거금률_1_00에서_16개_스레드가_동시에_보내도_통과는_10건이다() throws Exception {
        for (int round = 0; round < REPEAT; round++) {
            long accountId = FIRST_ACCOUNT_THREADS_16 + round;
            Outcome outcome = sendOrders(accountId, 16);
            assertOutcome(outcome, 10);
        }
    }

    @Test
    void 증거금률_0_40에서_8개_스레드가_동시에_보내도_통과는_25건이다() throws Exception {
        for (int round = 0; round < REPEAT; round++) {
            long accountId = FIRST_ACCOUNT_MARGIN_040 + round;
            Outcome outcome = sendOrders(accountId, 8);
            assertOutcome(outcome, 25);
        }
    }

    @Test
    void 대조_스레드_1개로_순차_발신해도_통과는_10건이다() throws Exception {
        for (int round = 0; round < REPEAT; round++) {
            long accountId = FIRST_ACCOUNT_SEQUENTIAL + round;
            Outcome outcome = sendOrders(accountId, 1);
            assertOutcome(outcome, 10);
        }
    }

    private void assertOutcome(Outcome outcome, int expectedAccepted) {
        assertThat(outcome.allArrived()).as("20초 안에 60건의 결과가 모두 도착해야 한다").isTrue();
        assertThat(outcome.accepted()).as("통과 건수").hasSize(expectedAccepted);
        assertThat(outcome.rejected()).as("거부 건수").hasSize(ORDER_COUNT - expectedAccepted);
        assertThat(outcome.rejected()).as("거부 이유").containsOnly(RejectReason.INSUFFICIENT);

        BigDecimal reservedSum = BigDecimal.ZERO;
        for (BigDecimal reserved : outcome.accepted()) {
            reservedSum = reservedSum.add(reserved);
        }
        assertThat(reservedSum).as("통과한 주문의 예약 증거금 합").isEqualByComparingTo(INITIAL_BALANCE);
        assertThat(outcome.duplicates()).as("중복 요청 판정").isZero();
    }

    /**
     * 한 계좌에 주문 60건을 threads개 스레드로 나눠 동시에 발신하고, 결과가 모두 도착할 때까지 기다린다.
     * 스레드 하나(threads=1)면 순차 발신이다. Publication은 모든 스레드가 하나를 공유한다.
     */
    private Outcome sendOrders(long accountId, int threads) throws Exception {
        recorder.prepare(accountId, ORDER_COUNT);
        Publication publication = aeron.addPublication(CHANNEL, AeronStreamIds.ACCOUNT_INTAKE);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            awaitConnected(publication);

            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> senders = new ArrayList<>();
            for (int thread = 0; thread < threads; thread++) {
                int threadIndex = thread;
                Future<?> sender = pool.submit(() -> {
                    sendShare(publication, accountId, threadIndex, threads, ready, start);
                    return null;
                });
                senders.add(sender);
            }

            assertThat(ready.await(10, TimeUnit.SECONDS)).as("발신 스레드 준비").isTrue();
            start.countDown();
            for (Future<?> sender : senders) {
                sender.get(30, TimeUnit.SECONDS);
            }

            boolean allArrived = recorder.await();
            return new Outcome(allArrived, recorder.accepted(), recorder.rejected(), recorder.duplicates());
        } finally {
            pool.shutdownNow();
            publication.close();
        }
    }

    /** 이 스레드가 맡은 주문(index % threads == threadIndex)을 시작 신호 뒤에 발신한다. */
    private void sendShare(Publication publication, long accountId, int threadIndex, int threads,
                           CountDownLatch ready, CountDownLatch start) throws InterruptedException {
        AccountOrderCodec codec = new AccountOrderCodec();
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        ready.countDown();
        start.await();
        for (int index = threadIndex; index < ORDER_COUNT; index += threads) {
            DecodedAccountOrder order = new DecodedAccountOrder(
                OrderSide.BUY, accountId, STOCK, ORDER_PRICE, ORDER_QUANTITY, "c-" + accountId + "-" + index,
                index + 1L);
            offerWithRetry(publication, codec, buffer, order);
        }
    }

    /** 주문을 인코딩해 발행한다. 백프레셔(반환값 0 이하)면 5초 안에서 재시도하고, 넘으면 실패시킨다. */
    private void offerWithRetry(Publication publication, AccountOrderCodec codec, UnsafeBuffer buffer,
                                DecodedAccountOrder order) {
        int length = codec.encode(buffer, 0, order);
        long deadline = System.nanoTime() + OFFER_TIMEOUT_NANOS;
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
        long deadline = System.nanoTime() + OFFER_TIMEOUT_NANOS;
        while (!publication.isConnected()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 Publication이 연결되지 않음");
            }
            Thread.yield();
        }
    }

    private record Outcome(boolean allArrived, List<BigDecimal> accepted, List<RejectReason> rejected,
                           int duplicates) {
    }

    static class RecorderConfig {
        @Bean
        Recorder recorder() {
            return new Recorder();
        }
    }

    /**
     * 엔진 업무 스레드에서 불리는 콜백을 모으는 {@link AccountResultListener}. 한 번에 한 계좌의 한 회차만
     * 기록한다({@link #prepare}가 목록을 새로 만든다). 다른 계좌의 콜백은 무시한다.
     */
    static class Recorder implements AccountResultListener {
        private volatile long targetAccountId;
        private volatile CountDownLatch latch;
        private volatile List<BigDecimal> accepted = new CopyOnWriteArrayList<>();
        private volatile List<RejectReason> rejected = new CopyOnWriteArrayList<>();
        private final AtomicInteger duplicates = new AtomicInteger();

        void prepare(long accountId, int expectedResults) {
            accepted = new CopyOnWriteArrayList<>();
            rejected = new CopyOnWriteArrayList<>();
            duplicates.set(0);
            targetAccountId = accountId;
            latch = new CountDownLatch(expectedResults);
        }

        boolean await() throws InterruptedException {
            return latch.await(20, TimeUnit.SECONDS);
        }

        List<BigDecimal> accepted() {
            return accepted;
        }

        List<RejectReason> rejected() {
            return rejected;
        }

        int duplicates() {
            return duplicates.get();
        }

        @Override
        public void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin) {
            if (accountId != targetAccountId) {
                return;
            }
            accepted.add(reservedMargin);
            latch.countDown();
        }

        @Override
        public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
        }

        @Override
        public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
            if (accountId != targetAccountId) {
                return;
            }
            rejected.add(reason);
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
            if (accountId != targetAccountId) {
                return;
            }
            duplicates.incrementAndGet();
        }
    }
}
