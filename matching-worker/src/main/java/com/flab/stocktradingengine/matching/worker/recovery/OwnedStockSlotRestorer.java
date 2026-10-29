package com.flab.stocktradingengine.matching.worker.recovery;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;

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

    /**
     * 스냅샷의 목록에서 시작해(스냅샷이 없으면 빈 목록), 저널의 「slot 받음」·「slot 넘김」 항목을 기록된 순서대로 적용한
     * 목록을 돌려준다(11-12절). 주문 항목은 목록에 영향을 주지 않는다.
     */
    public static Set<Integer> restoreFromJournal(Set<Integer> snapshotSlots, List<JournaledOrder> journalEntries) {
        Set<Integer> restored = new HashSet<>(snapshotSlots);
        for (JournaledOrder entry : journalEntries) {
            if (entry.type() == EventType.SLOT_ACQUIRED) {
                restored.add(entry.stockSlot());
            } else if (entry.type() == EventType.SLOT_RELEASED) {
                restored.remove(entry.stockSlot());
            }
        }
        return Set.copyOf(restored);
    }
}
