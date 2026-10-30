package com.flab.stocktradingengine.matching.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.journal.Journal;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.lmax.disruptor.BlockingWaitStrategy;

/** 12-3절 2번 표의 7단계: 빈 칸이 없어 발행 thread가 기다린 발행 호출 수를 누적으로 읽을 수 있다(짧게 0에 닿았다 풀린 구간도 남는다). */
class MatchingEngineRingFullWaitTest {

    /** 첫 append에서 문이 열릴 때까지 기다려 소비자 thread를 멈춘다. */
    private static final class BlockingJournal implements Journal {
        final CountDownLatch gate = new CountDownLatch(1);

        @Override
        public void append(JournaledOrder order) {
            try {
                gate.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public List<JournaledOrder> entries() {
            return List.of();
        }

        @Override
        public long position() {
            return 0L;
        }
    }

    @Test
    void 링버퍼가_가득_차_발행이_기다린_호출_수를_센다() throws Exception {
        BlockingJournal journal = new BlockingJournal();
        MatchingEngine engine = new MatchingEngine(4, new BlockingWaitStrategy(), (stockCode, fill) -> { }, journal);
        engine.start();
        assertThat(engine.ringFullWaitCount()).isZero();
        Thread producer = new Thread(() -> {
            for (long orderId = 1; orderId <= 12; orderId++) {
                engine.publishPlace(orderId, 1L, "005930", OrderSide.SELL, new BigDecimal("10000"), 1, Instant.now());
            }
        });
        producer.setDaemon(true);
        try {
            producer.start();
            long deadline = System.nanoTime() + 5_000_000_000L;
            while (engine.ringFullWaitCount() == 0 && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            assertThat(engine.ringFullWaitCount()).as("소비자가 멈춘 채 용량(4)을 넘겨 발행하면 기다린 호출이 생긴다").isPositive();
        } finally {
            journal.gate.countDown();
            producer.join(10_000);
            engine.shutdown();
        }
        assertThat(producer.isAlive()).isFalse();
        assertThat(engine.ringFullWaitCount()).as("호출 수이지 기다린 시간이 아니다").isLessThanOrEqualTo(12L);
    }

    @Test
    void 막혀서_기다린_총_시간을_나노초로_센다() throws Exception {
        BlockingJournal journal = new BlockingJournal();
        MatchingEngine engine = new MatchingEngine(4, new BlockingWaitStrategy(), (stockCode, fill) -> { }, journal);
        engine.start();
        assertThat(engine.ringFullWaitNanos()).as("기다린 적이 없으면 0").isZero();
        Thread producer = new Thread(() -> {
            for (long orderId = 1; orderId <= 12; orderId++) {
                engine.publishPlace(orderId, 1L, "005930", OrderSide.SELL, new BigDecimal("10000"), 1, Instant.now());
            }
        });
        producer.setDaemon(true);
        try {
            producer.start();
            long deadline = System.nanoTime() + 5_000_000_000L;
            while (engine.ringFullWaitCount() == 0 && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            assertThat(engine.ringFullWaitCount()).isPositive();
            Thread.sleep(150);
            journal.gate.countDown();
            producer.join(10_000);
            assertThat(producer.isAlive()).isFalse();
            assertThat(engine.ringFullWaitNanos())
                .as("소비자가 150ms 이상 멈춰 있어 발행이 그만큼 기다렸다")
                .isGreaterThanOrEqualTo(100_000_000L);
        } finally {
            journal.gate.countDown();
            engine.shutdown();
        }
    }
}
