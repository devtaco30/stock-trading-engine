package com.flab.stocktradingengine.matching.worker.recovery;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * 재시작 때 이 매칭 shard가 맡은 종목 slot 목록을 디스크에서 복원한다. 스냅샷에 적힌 목록에서,
 * 스냅샷 이후 저널에 적힌 「slot 넘김」 이벤트가 가리키는 종목 slot을 뺀다. Kafka의 첫 배정을
 * 기다리지 않으므로 복구가 Kafka에 의존하지 않는다. 이벤트에는 녹화 위치를 적지 않는다(11-4절).
 */
public final class OwnedStockSlotRestorer {

    private OwnedStockSlotRestorer() {
    }

    public static Set<Integer> restore(Set<Integer> snapshotSlots, Collection<Integer> handedOverSlots) {
        Set<Integer> restored = new HashSet<>(snapshotSlots);
        restored.removeAll(handedOverSlots);
        return Set.copyOf(restored);
    }
}
