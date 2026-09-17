package com.flab.stocktradingengine.codec;

/** 계좌 워커가 주문 접수 요청 하나에 내린 판정. */
public enum OrderVerdict {
    ACCEPTED,
    REJECTED,
    DUPLICATE
}
