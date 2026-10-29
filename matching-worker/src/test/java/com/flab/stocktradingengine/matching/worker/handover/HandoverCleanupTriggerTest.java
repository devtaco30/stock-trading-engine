package com.flab.stocktradingengine.matching.worker.handover;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.MatchingSlotTarget;

/**
 * 넘긴 쪽(X)이 맵 전환을 신호로 정리를 요청하는 조건(U16b, 11-14절 2번). 자기가 넘겨 보관 중인 종목 slot의 맵 값이 다른 pod의
 * 평상시 주소로 바뀌었을 때만 요청한다.
 */
class HandoverCleanupTriggerTest {

    private static final String OWN = "aeron:udp?endpoint=localhost:20020";
    private static final String OTHER = "aeron:udp?endpoint=localhost:20021";

    private final HandoverBookStore store = new HandoverBookStore();
    private final List<Integer> cleanupRequests = new ArrayList<>();
    private final HandoverCleanupTrigger trigger = new HandoverCleanupTrigger(OWN, store, cleanupRequests::add);

    @Test
    void 맵이_아직_이동_중이면_요청하지_않는다() {
        store.put(5, List.of(new byte[] {1}));

        trigger.onMapChange(5, Optional.of(new MatchingSlotTarget(OWN, true)));
        trigger.onMapChange(5, Optional.of(new MatchingSlotTarget(OTHER, true)));

        assertThat(cleanupRequests).isEmpty();
    }

    @Test
    void 맵이_다른_pod의_평상시_주소로_바뀌면_정리를_요청한다() {
        store.put(5, List.of(new byte[] {1}));

        trigger.onMapChange(5, Optional.of(new MatchingSlotTarget(OTHER, false)));

        assertThat(cleanupRequests).containsExactly(5);
    }

    @Test
    void 맵이_자기_주소의_평상시_값이면_요청하지_않는다() {
        store.put(5, List.of(new byte[] {1}));

        trigger.onMapChange(5, Optional.of(new MatchingSlotTarget(OWN, false)));

        assertThat(cleanupRequests).isEmpty();
    }

    @Test
    void 맵_항목이_사라져도_요청하지_않는다() {
        store.put(5, List.of(new byte[] {1}));

        trigger.onMapChange(5, Optional.empty());

        assertThat(cleanupRequests).isEmpty();
    }

    @Test
    void 자기가_넘겨_보관하지_않은_종목_slot의_변경은_무시한다() {
        trigger.onMapChange(7, Optional.of(new MatchingSlotTarget(OTHER, false)));

        assertThat(cleanupRequests).isEmpty();
    }
}
