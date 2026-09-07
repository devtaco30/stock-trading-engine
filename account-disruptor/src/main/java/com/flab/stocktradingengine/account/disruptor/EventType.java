package com.flab.stocktradingengine.account.disruptor;

/**
 * 링버퍼 슬롯이 담는 명령 종류.
 *
 * <p>BUY(매수 검증·예약) / BUY_FILL(매수 체결 반영) / SELL_FILL(매도 체결 반영).
 * matching-disruptor 의 {@code EventType}(PLACE/CANCEL)과 같은 이유로,
 * 하나의 {@link AccountEvent} 슬롯이 이 타입에 따라 계좌 핸들러에서 다르게 처리된다.</p>
 */
public enum EventType {
    BUY,
    BUY_FILL,
    SELL_FILL
}
