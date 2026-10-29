package com.flab.stocktradingengine.account.worker.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** 매칭 샤딩 U9b — 1초마다 로그로 내보내는 한 줄이 구간 증가분과 현재 쌓인 개수를 담는지 확인한다. */
class MatchingSendMetricsReporterTest {

    @Test
    void 이전_스냅샷과의_차이와_현재_쌓인_개수를_한_줄로_만든다() {
        MatchingSendMetrics previous = new MatchingSendMetrics(10, 2, 5_000_000L, 7);
        MatchingSendMetrics current = new MatchingSendMetrics(42, 5, 8_500_000L, 19);

        String line = MatchingSendMetricsReporter.formatLine(previous, current);

        assertThat(line).isEqualTo("matching-send outbox=42 enqueueWaits=3 enqueueWaitMs=3.5 offerRetries=12");
    }
}
