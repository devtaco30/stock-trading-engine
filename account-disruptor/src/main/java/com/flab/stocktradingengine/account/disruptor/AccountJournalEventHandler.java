package com.flab.stocktradingengine.account.disruptor;

import com.lmax.disruptor.EventHandler;

import com.flab.stocktradingengine.codec.AccountJournalEntry;

/**
 * 링버퍼를 소비해 이벤트를 계좌 저널에 기록하는 핸들러(2b-1) — matching {@code JournalEventHandler}의
 * 계좌판. 매칭과 달리 주문뿐 아니라 체결·정산까지 전 타입을 기록한다(계좌 상태를 만드는 모든 입력).
 *
 * <h3>비즈니스 처리보다 먼저 도는 이유</h3>
 * <p>{@code AccountEngine}에서 이 핸들러를 {@code handleEventsWith(journalHandler).then(businessHandler)}로
 * 배선한다. 비즈니스 핸들러({@link AccountEventHandler})는 이 핸들러가 기록을 마친 뒤에만 그
 * 이벤트를 처리한다. 그래서 순서가 항상 "먼저 기록 → 그다음 반영"이 되고, 기록되지 않은 입력이
 * 계좌 상태를 바꾸는 일이 없다.</p>
 *
 * <h3>슬롯을 비우지 않는 이유</h3>
 * <p>이 핸들러는 마지막 소비자가 아니다. 뒤이어 비즈니스 핸들러가 같은 슬롯을 읽어야 하므로
 * 여기서 {@code clear()}를 호출하지 않는다. 슬롯 비우기는 마지막 소비자(비즈니스 핸들러)가 맡는다.</p>
 */
public class AccountJournalEventHandler implements EventHandler<AccountEvent> {

    private final AccountJournal journal;

    public AccountJournalEventHandler(AccountJournal journal) {
        this.journal = journal;
    }

    @Override
    public void onEvent(AccountEvent event, long sequence, boolean endOfBatch) {
        journal.append(new AccountJournalEntry(
            event.getType(), event.getOrderId(), event.getAccountId(), event.getStockCode(),
            event.getPrice(), event.getQuantity(), event.getRequestId(), event.getTradeId()));
    }
}
