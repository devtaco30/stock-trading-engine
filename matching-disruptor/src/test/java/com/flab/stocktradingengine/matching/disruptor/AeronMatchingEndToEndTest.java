package com.flab.stocktradingengine.matching.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;

import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.matching.FillResult;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.codec.OrderCodec;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.io.AeronOrderReceiver;

/**
 * Unit 4c — Aeron 을 실제로 통과한 주문이 매칭돼 체결이 나오는지 검증(end-to-end).
 *
 * <p>조각을 합친다: 생산자가 주문을 {@link OrderCodec} 로 인코딩해 Aeron 으로 보내면(4a·4b),
 * {@link AeronOrderReceiver} 가 받아 디코딩하고 {@link MatchingEngine} 에 넣어(4c) 매칭이 돈다.
 * 인메모리 피더가 Aeron 으로 대체된 것을 이 테스트가 증명한다.</p>
 */
class AeronMatchingEndToEndTest {

    private static final String CHANNEL = "aeron:ipc";
    private static final int STREAM_ID = 2002;
    private static final int BUFFER_SIZE = 1024;
    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;

    private final OrderCodec codec = new OrderCodec();

    @Test
    void Aeron으로_들어온_교차_주문이_매칭돼_체결난다() throws InterruptedException {
        MediaDriver driver = MediaDriver.launchEmbedded(cleanEmbeddedMediaDriverContext());
        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));

        List<FillResult> fills = new CopyOnWriteArrayList<>();
        CountDownLatch fillLatch = new CountDownLatch(1);
        MatchingEngine engine = new MatchingEngine(BUFFER_SIZE, (stockCode, fill) -> {
            fills.add(fill);
            fillLatch.countDown();
        });
        engine.start();

        Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID);
        AeronOrderReceiver receiver = new AeronOrderReceiver(subscription, engine);
        receiver.start();

        Publication publication = aeron.addPublication(CHANNEL, STREAM_ID);

        try {
            awaitConnected(publication);

            Instant now = Instant.parse("2026-09-07T00:00:00Z");
            send(publication, new JournaledOrder(
                EventType.PLACE, 1L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 100, now));
            send(publication, new JournaledOrder(
                EventType.PLACE, 2L, 200L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 100, now.plusMillis(1)));

            boolean arrived = fillLatch.await(5, TimeUnit.SECONDS);

            assertTrue(arrived, "5초 안에 체결이 도착해야 한다");
            assertEquals(1, fills.size());
            FillResult fill = fills.get(0);
            assertEquals(1L, fill.buyOrderId());
            assertEquals(2L, fill.sellOrderId());
            assertEquals(100, fill.filledQuantity());
            assertEquals(0, new BigDecimal("10000").compareTo(fill.matchPrice()));
        } finally {
            publication.close();
            receiver.close();
            subscription.close();
            engine.shutdown();
            aeron.close();
            driver.close();
        }
    }

    /** 주문을 인코딩해 Aeron 으로 발행한다. 백프레셔면 받아들여질 때까지 재시도한다. */
    private void send(Publication publication, JournaledOrder order) {
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
                fail("5초 안에 Publication 이 연결되지 않음");
            }
            Thread.yield();
        }
    }

    /** Aeron 드라이버 통신용 임시 디렉터리(aeron-*)를 시작·종료 시 지운다(I10) — 기본값은 안 지운다. */
    private static MediaDriver.Context cleanEmbeddedMediaDriverContext() {
        return new MediaDriver.Context().dirDeleteOnStart(true).dirDeleteOnShutdown(true);
    }
}
