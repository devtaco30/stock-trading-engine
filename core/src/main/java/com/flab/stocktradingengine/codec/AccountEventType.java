package com.flab.stocktradingengine.codec;

/**
 * 계좌 저널(2b-1)이 기록하는 이벤트 종류 — account-disruptor 링버퍼 슬롯의 명령 종류였던
 * {@code account.disruptor.EventType}을 2b-1b에서 이 자리로 옮겼다({@link AccountJournalEntry}가
 * core.codec으로 옮겨가며 함께 옮김 — {@link AccountJournalEntryCodec}이 core 모듈에서 인코딩하려면
 * 이 타입도 core에 있어야 한다).
 *
 * <p>{@link EventType}(PLACE/CANCEL, 매칭 인테이크 명령)과는 다른 이벤트 종류라 이름을 분리했다.</p>
 */
public enum AccountEventType {
    BUY,
    SELL,
    BUY_FILL,
    SELL_FILL,
    SETTLEMENT
}
