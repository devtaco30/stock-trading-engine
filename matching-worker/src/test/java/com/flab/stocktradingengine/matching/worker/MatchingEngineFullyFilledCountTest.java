package com.flab.stocktradingengine.matching.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.trading.entity.OrderSide;

/** 12-3절 1번 표의 「바」: 호가창에 남지 않은(전량 체결된) 주문 수를 엔진이 센다. */
class MatchingEngineFullyFilledCountTest {

    @Test
    void 전량_체결된_주문을_센다() throws Exception {
        AtomicInteger fills = new AtomicInteger();
        MatchingEngine engine = new MatchingEngine(64, (stockCode, fill) -> fills.incrementAndGet());
        engine.start();
        try {
            assertThat(engine.fullyFilledOrderCount()).isZero();
            engine.publishPlace(1L, 1L, "005930", OrderSide.SELL, new BigDecimal("10000"), 5, Instant.now());
            engine.publishPlace(2L, 2L, "005930", OrderSide.SELL, new BigDecimal("10000"), 3, Instant.now());
            engine.publishPlace(3L, 3L, "005930", OrderSide.BUY, new BigDecimal("10000"), 8, Instant.now());
            engine.publishPlace(4L, 4L, "005930", OrderSide.BUY, new BigDecimal("9000"), 1, Instant.now()); // 체결 없이 호가창에 남는다

            long deadline = System.nanoTime() + 5_000_000_000L;
            while (fills.get() < 2 && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }

            assertThat(fills.get()).as("매수 8이 매도 5와 3을 차례로 체결").isEqualTo(2);
            assertThat(engine.fullyFilledOrderCount()).as("매도 1, 매도 2, 매수 3이 전량 체결되고 매수 4는 호가창에 남는다").isEqualTo(3L);
        } finally {
            engine.shutdown();
        }
    }
}
