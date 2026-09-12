package com.flab.stocktradingengine.matching.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.matching.FillResult;

/**
 * Unit 1 매칭 파이프라인 검증.
 *
 * <p>프로듀서(테스트)와 소비자(매칭 핸들러)가 서로 다른 스레드에서 돌기 때문에,
 * 발행 직후 결과가 준비돼 있지 않다. {@link CountDownLatch} 로 체결 도착을 기다린 뒤 검증한다.</p>
 */
class MatchingEngineTest {

    private static final String STOCK = "005930";
    private static final int BUFFER_SIZE = 1024;

    private final List<FillResult> fills = new CopyOnWriteArrayList<>();
    private MatchingEngine engine;
    private CountDownLatch latch;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    private void startEngine(int expectedFills) {
        latch = new CountDownLatch(expectedFills);
        engine = new MatchingEngine(BUFFER_SIZE, (stockCode, fill) -> {
            fills.add(fill);
            latch.countDown();
        });
        engine.start();
    }

    @Test
    void 교차_주문은_체결된다() throws InterruptedException {
        startEngine(1);
        Instant now = Instant.now();

        engine.publishPlace(1L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 100, now);
        engine.publishPlace(2L, 200L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 100, now.plusMillis(1));

        boolean arrived = latch.await(1, TimeUnit.SECONDS);

        assertTrue(arrived, "1초 안에 체결이 도착해야 한다");
        assertEquals(1, fills.size());
        FillResult fill = fills.get(0);
        assertEquals(1L, fill.buyOrderId());
        assertEquals(2L, fill.sellOrderId());
        assertEquals(100, fill.filledQuantity());
        assertEquals(0, new BigDecimal("10000").compareTo(fill.matchPrice()));
    }

    @Test
    void 가격이_안_맞으면_체결되지_않는다() throws InterruptedException {
        startEngine(1);
        Instant now = Instant.now();

        // 매수 9000 < 매도 10000 → 체결 불가
        engine.publishPlace(1L, 100L, STOCK, OrderSide.BUY, new BigDecimal("9000"), 100, now);
        engine.publishPlace(2L, 200L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 100, now.plusMillis(1));

        boolean arrived = latch.await(300, TimeUnit.MILLISECONDS);

        assertFalse(arrived, "체결이 없어야 한다");
        assertEquals(0, fills.size());
    }
}
