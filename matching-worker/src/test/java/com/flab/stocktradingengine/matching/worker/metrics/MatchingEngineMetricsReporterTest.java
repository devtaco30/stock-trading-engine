package com.flab.stocktradingengine.matching.worker.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.trading.entity.OrderSide;

/** 12-3절 2번 표의 7·8·9단계 값을 1초마다 로그 한 줄로 낸다. 계좌 쪽 account-engine 줄과 같은 모양이다. */
class MatchingEngineMetricsReporterTest {

    @Test
    void 누적_값은_구간_증가분으로_남은_칸과_queue_깊이는_현재_값으로_한_줄을_만든다() {
        MatchingEngineMetrics previous = new MatchingEngineMetrics(1024, 3, 10, 4, 7, 2);
        MatchingEngineMetrics current = new MatchingEngineMetrics(0, 8, 25, 9, 65536, 5);

        assertThat(MatchingEngineMetricsReporter.formatLine(previous, current)).isEqualTo(
            "matching-engine ringRemaining=0 ringFullWaits=5 journalBackPressure=15 journalOtherRetries=5 fillQueueDepth=65536 fillEnqueueWaits=3");
    }

    @Test
    void 엔진과_저널과_체결_queue에서_값을_읽는다() {
        MatchingEngine engine = new MatchingEngine(8, (stockCode, fill) -> { });
        AtomicLong depth = new AtomicLong(11);
        AtomicLong waits = new AtomicLong(2);

        MatchingEngineMetrics beforeStart = MatchingEngineMetricsReporter.read(engine, depth::get, waits::get);
        assertThat(beforeStart.ringRemaining()).as("시작 전에는 링버퍼가 없어 -1").isEqualTo(-1L);
        assertThat(beforeStart.fillQueueDepth()).isEqualTo(11L);
        assertThat(beforeStart.fillEnqueueWaits()).isEqualTo(2L);
        assertThat(beforeStart.journalBackPressure()).as("Aeron 저널이 아니면 저널 재시도는 0").isZero();
        assertThat(beforeStart.journalOtherRetries()).isZero();

        engine.start();
        try {
            engine.publishPlace(1L, 1L, "005930", OrderSide.SELL, new BigDecimal("10000"), 1, Instant.now());
            MatchingEngineMetrics afterStart = MatchingEngineMetricsReporter.read(engine, depth::get, waits::get);
            assertThat(afterStart.ringRemaining()).isBetween(0L, 8L);
            assertThat(afterStart.ringFullWaits()).isZero();
        } finally {
            engine.shutdown();
        }
    }
}
