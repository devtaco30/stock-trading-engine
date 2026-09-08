package com.flab.stocktradingengine.account.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;

import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.wire.AccountOrderCodec;
import com.flab.stocktradingengine.wire.DecodedAccountOrder;

/**
 * C5-1b — Aeron을 실제로 통과한 매수·매도 주문이 검증·예약까지 되는지 검증(end-to-end).
 *
 * <p>matching-disruptor의 {@code AeronMatchingEndToEndTest}와 같은 결. 생산자가 주문을
 * {@link AccountOrderCodec}로 인코딩해 Aeron으로 보내면, {@link AccountOrderReceiver}가 받아
 * 디코딩하고 {@link AccountEngine}에 넣어(publishBuy/publishSell) 검증·예약이 돈다.</p>
 */
class AeronAccountEndToEndTest {

    private static final String CHANNEL = "aeron:ipc";
    private static final int STREAM_ID = 3003;
    private static final int BUFFER_SIZE = 1024;
    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;

    private final AccountOrderCodec codec = new AccountOrderCodec();

    @Test
    void Aeron으로_들어온_매수주문이_검증되고_예약된다() throws InterruptedException {
        MediaDriver driver = MediaDriver.launchEmbedded();
        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));

        List<Recorded> events = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        AccountEngine engine = new AccountEngine(BUFFER_SIZE, new AtomicLong(0)::incrementAndGet, new Recorder(events, latch));
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID);
        AccountOrderReceiver receiver = new AccountOrderReceiver(subscription, engine);
        receiver.start();

        Publication publication = aeron.addPublication(CHANNEL, STREAM_ID);

        try {
            awaitConnected(publication);

            send(publication, new DecodedAccountOrder(
                OrderSide.BUY, 1L, STOCK, new BigDecimal("10000"), 10, "req-1"));

            assertTrue(latch.await(5, TimeUnit.SECONDS), "5초 안에 결과가 도착해야 한다");
            assertEquals(1, events.size());
            Recorded event = events.get(0);
            assertTrue(event.accepted());
            assertEquals(0, event.reservedMargin().compareTo(new BigDecimal("40000")));
            assertEquals(1L, event.orderId()); // 발신자는 orderId를 안 보냈는데도 계좌 워커가 발급해 돌려준다(C5-2a)
        } finally {
            publication.close();
            receiver.close();
            subscription.close();
            engine.shutdown();
            aeron.close();
            driver.close();
        }
    }

    @Test
    void 같은_requestId로_두번_발신하면_둘째는_재예약없이_중복통지된다() throws InterruptedException {
        MediaDriver driver = MediaDriver.launchEmbedded();
        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));

        List<Recorded> events = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(2);
        AccountEngine engine = new AccountEngine(BUFFER_SIZE, new AtomicLong(0)::incrementAndGet, new Recorder(events, latch));
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID + 1);
        AccountOrderReceiver receiver = new AccountOrderReceiver(subscription, engine);
        receiver.start();

        Publication publication = aeron.addPublication(CHANNEL, STREAM_ID + 1);

        try {
            awaitConnected(publication);

            send(publication, new DecodedAccountOrder(
                OrderSide.BUY, 1L, STOCK, new BigDecimal("10000"), 10, "req-dup"));
            send(publication, new DecodedAccountOrder(
                OrderSide.BUY, 1L, STOCK, new BigDecimal("10000"), 10, "req-dup")); // 같은 requestId 재전송

            assertTrue(latch.await(5, TimeUnit.SECONDS), "5초 안에 결과 2개가 도착해야 한다");
            assertEquals(2, events.size());
            assertTrue(events.get(0).accepted());
            assertTrue(events.get(1).duplicate());
            assertEquals(events.get(0).orderId(), events.get(1).orderId()); // 재전송도 같은 orderId(C5-2a)
        } finally {
            publication.close();
            receiver.close();
            subscription.close();
            engine.shutdown();
            aeron.close();
            driver.close();
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
                fail("5초 안에 주문 발행 실패 — offer 반환=" + result);
            }
            Thread.yield();
        } while (true);
    }

    private void awaitConnected(Publication publication) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (!publication.isConnected()) {
            if (System.nanoTime() > deadline) {
                fail("5초 안에 Publication이 연결되지 않음");
            }
            Thread.yield();
        }
    }

    private record Recorded(boolean accepted, long orderId, BigDecimal reservedMargin, boolean duplicate) {
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
            events.add(new Recorded(true, orderId, reservedMargin, false));
            latch.countDown();
        }

        @Override
        public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
            events.add(new Recorded(true, orderId, null, false));
            latch.countDown();
        }

        @Override
        public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
            events.add(new Recorded(false, orderId, null, false));
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
            events.add(new Recorded(false, orderId, null, true));
            latch.countDown();
        }
    }
}
