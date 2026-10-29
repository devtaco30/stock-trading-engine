package com.flab.stocktradingengine.matching.worker.handover;

import java.util.Optional;
import java.util.function.IntConsumer;

import com.flab.stocktradingengine.aeron.MatchingSlotTarget;

/**
 * 호가창을 넘긴 쪽(X)이 맵 전환을 보고 정리를 요청하는 조건(11-14절 2번). 맵 읽기가 반영한 변경 중, 자기가 넘겨 보관({@link
 * HandoverBookStore})하고 있는 종목 slot의 값이 다른 pod의 평상시 주소({@code Y주소}, {@code ;moving}이 없고 자기 주소가 아님)로
 * 바뀐 것이 새 담당(Y)이 호가창을 확정하고 담당을 켠 신호다. 이동 중 값과 자기 주소의 값은 신호가 아니다.
 */
public final class HandoverCleanupTrigger {

    private final String ownEndpoint;
    private final HandoverBookStore store;
    private final IntConsumer cleanupRequests;

    public HandoverCleanupTrigger(String ownEndpoint, HandoverBookStore store, IntConsumer cleanupRequests) {
        this.ownEndpoint = ownEndpoint;
        this.store = store;
        this.cleanupRequests = cleanupRequests;
    }

    /** 맵 읽기 thread에서 불린다. */
    public void onMapChange(int stockSlot, Optional<MatchingSlotTarget> target) {
        if (target.isEmpty() || target.get().moving() || target.get().endpoint().equals(ownEndpoint)) {
            return;
        }
        if (store.get(stockSlot).isEmpty()) {
            return;
        }
        cleanupRequests.accept(stockSlot);
    }
}
