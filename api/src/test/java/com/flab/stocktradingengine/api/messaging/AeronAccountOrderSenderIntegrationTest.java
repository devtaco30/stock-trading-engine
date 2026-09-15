package com.flab.stocktradingengine.api.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.codec.AccountOrderCodec;
import com.flab.stocktradingengine.codec.DecodedAccountOrder;
import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;

/**
 * fork5, U1b — 같은 JVM 임베디드 드라이버 위에 api 발행 스트림과 계좌 인테이크 구독을 같은
 * 채널·스트림으로 붙여, {@link AeronAccountOrderSender}가 보낸 바이트를 그대로 디코딩할 수
 * 있는지 확인한다(end-to-end, 매수·매도 둘 다). 실제 두 프로세스(api↔account-worker) 연결과
 * accountId 라우팅은 C5 몫 — 이 테스트는 발신 로직·인코딩까지만 검증한다.
 *
 * <p>{@code AeronAccountEndToEndTest}(account-disruptor)와 같은 결이되, 계좌 엔진까지는
 * 태우지 않고 구독에서 직접 {@link AccountOrderCodec#decode}해 필드만 비교한다.</p>
 */
class AeronAccountOrderSenderIntegrationTest {

    private static final String CHANNEL = "aeron:ipc";
    private static final int STREAM_ID = 4004;
    private static final long TIMEOUT_NANOS = 5_000_000_000L;
    private static final String STOCK_CODE = "005930";

    private final AccountOrderCodec codec = new AccountOrderCodec();

    private MediaDriver driver;
    private Aeron aeron;
    private Subscription subscription;
    private Publication publication;

    @BeforeEach
    void setUp() {
        driver = MediaDriver.launchEmbedded();
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        subscription = aeron.addSubscription(CHANNEL, STREAM_ID);
        publication = aeron.addPublication(CHANNEL, STREAM_ID);
        awaitConnected(publication);
    }

    @AfterEach
    void tearDown() {
        publication.close();
        subscription.close();
        aeron.close();
        driver.close();
    }

    @Test
    void 매수_주문을_발신하면_계좌_인테이크_구독에서_그대로_디코딩된다() {
        AeronAccountOrderSender sender = new AeronAccountOrderSender(publication);

        sender.send(OrderSide.BUY, 1L, STOCK_CODE, new BigDecimal("10000"), 10, "req-buy-1");

        DecodedAccountOrder decoded = awaitOne();
        assertThat(decoded).isEqualTo(new DecodedAccountOrder(OrderSide.BUY, 1L, STOCK_CODE, new BigDecimal("10000"), 10, "req-buy-1"));
    }

    @Test
    void 매도_주문을_발신하면_계좌_인테이크_구독에서_그대로_디코딩된다() {
        AeronAccountOrderSender sender = new AeronAccountOrderSender(publication);

        sender.send(OrderSide.SELL, 2L, STOCK_CODE, new BigDecimal("11000"), 5, "req-sell-1");

        DecodedAccountOrder decoded = awaitOne();
        assertThat(decoded).isEqualTo(new DecodedAccountOrder(OrderSide.SELL, 2L, STOCK_CODE, new BigDecimal("11000"), 5, "req-sell-1"));
    }

    /**
     * 싱글톤 빈 하나에 여러 HTTP 요청 스레드가 동시에 send()를 부르는 실제 상황을 재현한다
     * — 인코딩 버퍼를 스레드로컬로 재사용해도(39 리뷰) 동시 호출끼리 내용이 안 섞이는지 검증한다.
     * 필드로 그냥 재사용했다면 이 테스트가 뒤섞인 필드(다른 스레드의 accountId·requestId)를
     * 잡아냈을 것이다.
     */
    @Test
    void 동시_다발_발신도_인코딩이_섞이지_않는다() throws InterruptedException {
        AeronAccountOrderSender sender = new AeronAccountOrderSender(publication);
        int threadCount = 20;
        List<DecodedAccountOrder> expected = new CopyOnWriteArrayList<>();
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);

        for (int i = 0; i < threadCount; i++) {
            long accountId = i + 1L;
            int quantity = i + 1;
            String requestId = "req-concurrent-" + i;
            expected.add(new DecodedAccountOrder(OrderSide.BUY, accountId, STOCK_CODE, new BigDecimal("10000"), quantity, requestId));
            pool.submit(() -> {
                ready.countDown();
                awaitStart(start);
                sender.send(OrderSide.BUY, accountId, STOCK_CODE, new BigDecimal("10000"), quantity, requestId);
            });
        }
        assertTrue(ready.await(5, TimeUnit.SECONDS), "스레드 풀이 5초 안에 준비되지 않음");
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS), "발신 스레드들이 5초 안에 끝나지 않음");

        List<DecodedAccountOrder> received = awaitN(threadCount);
        assertThat(received).containsExactlyInAnyOrderElementsOf(expected);
    }

    private DecodedAccountOrder awaitOne() {
        return awaitN(1).get(0);
    }

    private List<DecodedAccountOrder> awaitN(int count) {
        List<DecodedAccountOrder> received = new ArrayList<>();
        FragmentHandler handler = (buffer, offset, length, header) -> {
            DirectBuffer directBuffer = buffer;
            received.add(codec.decode(directBuffer, offset));
        };

        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (received.size() < count) {
            subscription.poll(handler, count);
            if (received.size() < count && System.nanoTime() > deadline) {
                throw new AssertionError(count + "건 중 " + received.size() + "건만 5초 안에 도착함");
            }
        }
        return received;
    }

    private void awaitStart(CountDownLatch start) {
        try {
            start.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("대기 중 인터럽트됨", e);
        }
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
}
