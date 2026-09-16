package com.flab.stocktradingengine.time;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class EpochNanosTest {

    @Test
    @DisplayName("Instant을 초·나노 성분으로 나눠 하나의 epoch 나노초 long으로 합친다")
    void of_초와_나노를_합친다() {
        Instant instant = Instant.ofEpochSecond(1_700_000_000L, 123_456_789L);

        long epochNanos = EpochNanos.of(instant);

        assertEquals(1_700_000_000_123_456_789L, epochNanos);
    }

    @Test
    @DisplayName("에폭 원점은 0이다")
    void of_에폭_원점은_0() {
        assertEquals(0L, EpochNanos.of(Instant.EPOCH));
    }

    @Test
    @DisplayName("now()는 현재 시각 기준 epoch 나노초를 돌려준다 — Instant.now() 변환값과 초 단위로 일치")
    void now_현재시각_기준() {
        long before = EpochNanos.of(Instant.now());
        long epochNanos = EpochNanos.now();
        long after = EpochNanos.of(Instant.now());

        assertTrue(epochNanos >= before && epochNanos <= after);
    }
}
