package com.flab.stocktradingengine.codec;

/**
 * 매칭 인테이크 와이어가 담는 명령 종류.
 *
 * <p>PLACE(주문 접수) / CANCEL(주문 취소). {@code matching-disruptor}의 {@code OrderEvent} 슬롯이
 * 이 타입에 따라 매칭 핸들러에서 다르게 처리된다. 계좌 인테이크(매수·매도)는 이 타입을 쓰지 않고
 * {@link com.flab.stocktradingengine.trading.entity.OrderSide}를 쓴다 — PLACE/CANCEL과 BUY/SELL은
 * 서로 다른 인테이크의 명령 종류라 섞지 않는다.</p>
 */
public enum EventType {
    PLACE,
    CANCEL
}
