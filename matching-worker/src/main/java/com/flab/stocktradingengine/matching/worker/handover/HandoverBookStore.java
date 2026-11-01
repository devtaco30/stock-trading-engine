package com.flab.stocktradingengine.matching.worker.handover;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 이전 담당(X)이 넘김 이벤트 때 만든 종목별 호가창 바이트를 종목 slot별로 한 벌씩 들고 있는다(11-13절 4번). 요청이 오면
 * 호가창 발신 thread가 이 바이트를 꺼내 보낸다. 같은 종목 slot에 새 넘김이 오면 이전 한 벌을 교체하고, 그 종목 slot을 다시 받으면
 * 지운다. 넘김 이벤트를 처리하는 엔진 thread가 넣고 요청을 처리하는 thread가 읽으므로 동시성 맵이다.
 */
public final class HandoverBookStore {

    private final Map<Integer, List<byte[]>> bytesBySlot = new ConcurrentHashMap<>();
    // 보관했다가 지운 종목 slot. 지운 뒤 늦게 도착한 중복 요청과 한 번도 보관한 적 없는 종목 slot의 요청을 가르는 데 쓴다.
    private final java.util.Set<Integer> removedSlots = ConcurrentHashMap.newKeySet();

    public void put(int stockSlot, List<byte[]> bookBytesPerStock) {
        bytesBySlot.put(stockSlot, List.copyOf(bookBytesPerStock));
        removedSlots.remove(stockSlot);
    }

    public Optional<List<byte[]>> get(int stockSlot) {
        return Optional.ofNullable(bytesBySlot.get(stockSlot));
    }

    public void remove(int stockSlot) {
        if (bytesBySlot.remove(stockSlot) != null) {
            removedSlots.add(stockSlot);
        }
    }

    /** 보관했던 호가창을 정리로 지운 종목 slot인지. */
    public boolean wasRemoved(int stockSlot) {
        return removedSlots.contains(stockSlot);
    }
}
