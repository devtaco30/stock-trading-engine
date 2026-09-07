package com.flab.stocktradingengine.matching.disruptor;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 인메모리 저널.
 *
 * <p>저널러 스레드 하나가 {@link #append} 로 쓰고, 다른 스레드(예: 테스트·조회)가 {@link #entries}
 * 로 읽을 수 있다. 쓰기가 단일 스레드라도 읽기 스레드가 따로 있으므로,
 * 안전한 동시 접근을 위해 {@link CopyOnWriteArrayList} 를 쓴다.</p>
 */
public class InMemoryJournal implements Journal {

    private final List<JournaledOrder> entries = new CopyOnWriteArrayList<>();

    @Override
    public void append(JournaledOrder order) {
        entries.add(order);
    }

    @Override
    public List<JournaledOrder> entries() {
        return List.copyOf(entries);
    }
}
