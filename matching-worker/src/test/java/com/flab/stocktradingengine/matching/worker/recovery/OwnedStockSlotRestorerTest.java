package com.flab.stocktradingengine.matching.worker.recovery;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;


class OwnedStockSlotRestorerTest {

    @Test
    void 스냅샷_이후_slot_넘김이_가리키는_종목_slot이_복원한_목록에서_빠진다() {
        Set<Integer> snapshotSlots = Set.of(1, 2, 3, 4);

        Set<Integer> restored = OwnedStockSlotRestorer.restore(snapshotSlots, List.of(2, 4));

        assertThat(restored).containsExactlyInAnyOrder(1, 3);
    }

    @Test
    void 넘김이_없으면_스냅샷_목록_그대로이고_목록에_없는_slot의_넘김은_무시한다() {
        assertThat(OwnedStockSlotRestorer.restore(Set.of(1, 2), List.of())).containsExactlyInAnyOrder(1, 2);
        assertThat(OwnedStockSlotRestorer.restore(Set.of(1, 2), List.of(9))).containsExactlyInAnyOrder(1, 2);
    }
}
