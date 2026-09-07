package com.flab.stocktradingengine.matching.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * Unit 2 저널 게이팅 검증.
 *
 * <p>{@code handleEventsWith(journal).then(matcher)} 배선이 실제로
 * "먼저 기록 → 그다음 매칭" 순서를 지키는지를 확인한다.</p>
 */
class JournalGatingTest {

    private static final String STOCK = "005930";
    private static final int BUFFER_SIZE = 1024;
    private static final long BUY_ORDER_ID = 101L;
    private static final long SELL_ORDER_ID = 102L;

    private MatchingEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    @Test
    void 모든_주문은_저널에_순서대로_남는다() throws InterruptedException {
        CountDownLatch fillLatch = new CountDownLatch(1);
        engine = new MatchingEngine(BUFFER_SIZE, (stockCode, fill) -> fillLatch.countDown());
        engine.start();

        Instant now = Instant.now();
        engine.publishPlace(BUY_ORDER_ID, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 100, now);
        engine.publishPlace(SELL_ORDER_ID, 200L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 100, now.plusMillis(1));

        boolean arrived = fillLatch.await(1, TimeUnit.SECONDS);
        assertTrue(arrived, "1초 안에 체결이 도착해야 한다");

        List<JournaledOrder> entries = engine.journal().entries();
        assertEquals(2, entries.size());
        assertEquals(BUY_ORDER_ID, entries.get(0).orderId());
        assertEquals(SELL_ORDER_ID, entries.get(1).orderId());
    }

    @Test
    void 체결_시점엔_이미_저널됨() throws InterruptedException {
        CountDownLatch fillLatch = new CountDownLatch(1);
        AtomicInteger journalSizeAtFill = new AtomicInteger(-1);
        engine = new MatchingEngine(BUFFER_SIZE, (stockCode, fill) -> {
            // 체결 콜백은 매처(MatchingEventHandler) 스레드에서 호출된다.
            // 저널러가 매처보다 먼저 돈다면, 이 시점엔 두 주문이 이미 저널에 남아 있어야 한다.
            journalSizeAtFill.set(engine.journal().entries().size());
            fillLatch.countDown();
        });
        engine.start();

        Instant now = Instant.now();
        engine.publishPlace(BUY_ORDER_ID, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 100, now);
        engine.publishPlace(SELL_ORDER_ID, 200L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 100, now.plusMillis(1));

        boolean arrived = fillLatch.await(1, TimeUnit.SECONDS);
        assertTrue(arrived, "1초 안에 체결이 도착해야 한다");
        assertTrue(journalSizeAtFill.get() >= 2, "체결 시점엔 두 주문이 이미 저널에 있어야 한다");
    }
}
