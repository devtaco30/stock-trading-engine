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

    public void put(int stockSlot, List<byte[]> bookBytesPerStock) {
        bytesBySlot.put(stockSlot, List.copyOf(bookBytesPerStock));
    }

    public Optional<List<byte[]>> get(int stockSlot) {
        return Optional.ofNullable(bytesBySlot.get(stockSlot));
    }

    public void remove(int stockSlot) {
        bytesBySlot.remove(stockSlot);
    }
}
