package com.flab.stocktradingengine.account.disruptor;

import java.util.List;

/**
 * 계좌 입력 기록 저장소(2b-1) — matching {@code Journal}의 계좌판.
 *
 * <p>계좌 상태를 만드는 모든 입력(주문 접수·체결 반영·정산 되돌림)을 처리 순서대로 남긴다.
 * 프로세스가 죽어 인메모리 계좌 상태가 사라져도, 이 기록을 처음부터 재생(replay)하면 같은
 * 상태를 다시 만들 수 있다(2b-2). 지금은 인메모리 구현({@link InMemoryAccountJournal})만 두고,
 * 이후 Aeron Archive durable 구현(2b-1b)으로 교체할 수 있도록 인터페이스로 분리한다.</p>
 */
public interface AccountJournal {

    /** 이벤트 스냅샷을 기록 끝에 덧붙인다. */
    void append(AccountJournalEntry entry);

    /** 기록된 이벤트를 처리 순서대로 반환한다(복사본). */
    List<AccountJournalEntry> entries();
}
