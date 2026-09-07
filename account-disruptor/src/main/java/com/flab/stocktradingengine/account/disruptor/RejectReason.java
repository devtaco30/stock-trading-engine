package com.flab.stocktradingengine.account.disruptor;

/** 매수 검증 거부 사유. */
public enum RejectReason {
    ACCOUNT_NOT_FOUND,  // 워커가 소유하지 않은 계좌
    INSUFFICIENT,       // 매수 가능 금액 초과
    INVALID_QUANTITY    // 수량·가격이 유효하지 않음
}
