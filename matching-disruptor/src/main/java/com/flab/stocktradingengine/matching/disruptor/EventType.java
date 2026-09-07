package com.flab.stocktradingengine.matching.disruptor;

/**
 * 링버퍼 슬롯이 담는 명령 종류.
 *
 * <p>PLACE(주문 접수) / CANCEL(주문 취소). 하나의 {@link OrderEvent} 슬롯이
 * 이 타입에 따라 매칭 핸들러에서 다르게 처리된다.</p>
 */
public enum EventType {
    PLACE,
    CANCEL
}
