package com.flab.stocktradingengine.matching.worker.config;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * 맵 읽기가 예외로 멈춘 뒤 다시 시작하기까지의 간격에는 하한이 있다. 간격이 0에 가까우면 멈춤과 복구가 판정의 분류와 재확인 사이에 끝나
 * 멈춘 동안 받은 빈 값을 정상 값으로 놓칠 수 있다.
 */
class MapReaderRestartIntervalTest {

    @Test
    void 하한_미만이면_기동_예외다() {
        assertThatThrownBy(() -> MatchingHandoverConfig.requireValidReaderRestartMillis(99L))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("map-reader-restart-ms")
            .hasMessageContaining("100");
        assertThatThrownBy(() -> MatchingHandoverConfig.requireValidReaderRestartMillis(0L))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void 하한_이상이면_그대로_돌려준다() {
        assertThatCode(() -> MatchingHandoverConfig.requireValidReaderRestartMillis(100L)).doesNotThrowAnyException();
        org.assertj.core.api.Assertions.assertThat(MatchingHandoverConfig.requireValidReaderRestartMillis(5_000L)).isEqualTo(5_000L);
    }
}
