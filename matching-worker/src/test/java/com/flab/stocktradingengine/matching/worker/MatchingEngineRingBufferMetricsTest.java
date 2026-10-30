package com.flab.stocktradingengine.matching.worker;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;

/** 12-3절 2번 표의 7단계: 매칭 ring buffer의 남은 칸 수를 읽을 수 있다. */
class MatchingEngineRingBufferMetricsTest {

    @Test
    void 시작하기_전에는_알_수_없어_음수이고_시작한_뒤_처리할_주문이_없으면_전체_칸이_비어_있다() throws Exception {
        MatchingEngine engine = new MatchingEngine(1024, (stockCode, fill) -> { });
        assertThat(engine.ringBufferRemainingCapacity()).isEqualTo(-1L);

        engine.start();
        try {
            assertThat(engine.ringBufferRemainingCapacity()).isEqualTo(1024L);
        } finally {
            engine.shutdown();
        }
    }
}
