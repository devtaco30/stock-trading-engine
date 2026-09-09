package com.flab.stocktradingengine.account.disruptor;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 인메모리 계좌 저널(2b-1) — matching {@code InMemoryJournal}의 계좌판.
 *
 * <p>저널러 스레드 하나가 {@link #append}로 쓰고, 다른 스레드(예: 테스트·조회)가 {@link #entries}로
 * 읽을 수 있다. 쓰기가 단일 스레드라도 읽기 스레드가 따로 있으므로, 안전한 동시 접근을 위해
 * {@link CopyOnWriteArrayList}를 쓴다.</p>
 */
public class InMemoryAccountJournal implements AccountJournal {

    private final List<AccountJournalEntry> entries = new CopyOnWriteArrayList<>();

    @Override
    public void append(AccountJournalEntry entry) {
        entries.add(entry);
    }

    @Override
    public List<AccountJournalEntry> entries() {
        return List.copyOf(entries);
    }
}
