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
        MatchingEngineMetrics previous = new MatchingEngineMetrics(1024, 3, 10, 4, 7, 2, 100, 1000, 20, 15, 400);
        MatchingEngineMetrics current = new MatchingEngineMetrics(0, 8, 25, 9, 65536, 5, 160, 1900, 50, 40, 700);

        assertThat(MatchingEngineMetricsReporter.formatLine(previous, current)).isEqualTo(
            "matching-engine ringRemaining=0 ringFullWaits=5 journalBackPressure=15 journalOtherRetries=5 fillQueueDepth=65536 fillEnqueueWaits=3 fullyFilledOrders=60 fullyFilledOrdersTotal=160 ordersReceived=900 ordersNotOwned=30 rejectRepliesSent=25 fills=300");
    }

    @Test
    void 엔진과_저널과_체결_queue에서_값을_읽는다() {
        MatchingEngine engine = new MatchingEngine(8, (stockCode, fill) -> { });
        AtomicLong depth = new AtomicLong(11);
        AtomicLong waits = new AtomicLong(2);
        AtomicLong received = new AtomicLong(9);
        AtomicLong notOwned = new AtomicLong(4);
        AtomicLong repliesSent = new AtomicLong(3);

        MatchingEngineMetrics beforeStart = MatchingEngineMetricsReporter.read(engine, depth::get, waits::get, received::get, notOwned::get, repliesSent::get);
        assertThat(beforeStart.ringRemaining()).as("시작 전에는 링버퍼가 없어 -1").isEqualTo(-1L);
        assertThat(beforeStart.fillQueueDepth()).isEqualTo(11L);
        assertThat(beforeStart.fillEnqueueWaits()).isEqualTo(2L);
        assertThat(beforeStart.ordersReceived()).isEqualTo(9L);
        assertThat(beforeStart.ordersNotOwned()).isEqualTo(4L);
        assertThat(beforeStart.rejectRepliesSent()).isEqualTo(3L);
        assertThat(beforeStart.fills()).isZero();
        assertThat(beforeStart.journalBackPressure()).as("Aeron 저널이 아니면 저널 재시도는 0").isZero();
        assertThat(beforeStart.journalOtherRetries()).isZero();

        engine.start();
        try {
            engine.publishPlace(1L, 1L, "005930", OrderSide.SELL, new BigDecimal("10000"), 1, Instant.now());
            MatchingEngineMetrics afterStart = MatchingEngineMetricsReporter.read(engine, depth::get, waits::get, received::get, notOwned::get, repliesSent::get);
            assertThat(afterStart.ringRemaining()).isBetween(0L, 8L);
            assertThat(afterStart.ringFullWaits()).isZero();
        } finally {
            engine.shutdown();
        }
    }
}
