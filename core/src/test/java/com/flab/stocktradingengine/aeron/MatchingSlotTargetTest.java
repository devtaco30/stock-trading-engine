package com.flab.stocktradingengine.aeron;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 맵 토픽 {@code matching-shard-map}의 값 형식(`주소`, `주소;moving`)을 읽고 쓰는 쪽이 한 정의를 쓰는지 확인한다. */
class MatchingSlotTargetTest {

    private static final String ENDPOINT = "aeron:udp?endpoint=localhost:20020";

    @Test
    @DisplayName("평상시 값은 주소만이고 이동 중 표시가 없다")
    void 평상시_값을_읽는다() {
        MatchingSlotTarget target = MatchingSlotTarget.parse(ENDPOINT);

        assertEquals(ENDPOINT, target.endpoint());
        assertFalse(target.moving());
    }

    @Test
    @DisplayName("주소 뒤에 ;moving이 붙으면 이동 중이고 주소에서는 떼어낸다")
    void 이동_중_값을_읽는다() {
        MatchingSlotTarget target = MatchingSlotTarget.parse(ENDPOINT + ";moving");

        assertEquals(ENDPOINT, target.endpoint());
        assertTrue(target.moving());
    }

    @Test
    @DisplayName("쓰는 직렬화와 읽는 파싱이 서로 되돌린다")
    void 직렬화와_파싱이_왕복한다() {
        assertEquals(ENDPOINT, new MatchingSlotTarget(ENDPOINT, false).encode());
        assertEquals(ENDPOINT + ";moving", new MatchingSlotTarget(ENDPOINT, true).encode());
        assertEquals(new MatchingSlotTarget(ENDPOINT, true), MatchingSlotTarget.parse(new MatchingSlotTarget(ENDPOINT, true).encode()));
        assertEquals(new MatchingSlotTarget(ENDPOINT, false), MatchingSlotTarget.parse(new MatchingSlotTarget(ENDPOINT, false).encode()));
    }

    @Test
    @DisplayName("빈 값과 빈 주소는 거부한다")
    void 빈_값은_거부한다() {
        assertThrows(IllegalArgumentException.class, () -> MatchingSlotTarget.parse(null));
        assertThrows(IllegalArgumentException.class, () -> MatchingSlotTarget.parse(" "));
        assertThrows(IllegalArgumentException.class, () -> MatchingSlotTarget.parse(";moving"));
        assertThrows(IllegalArgumentException.class, () -> new MatchingSlotTarget(" ", false));
    }
}
