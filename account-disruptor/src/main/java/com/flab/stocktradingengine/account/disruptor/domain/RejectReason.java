package com.flab.stocktradingengine.account.disruptor.domain;

/** 매수·매도 검증 거부 사유. */
public enum RejectReason {
    ACCOUNT_NOT_FOUND,     // 워커가 소유하지 않은 계좌
    INSUFFICIENT,          // 매수 가능 금액 초과
    INVALID_QUANTITY,      // 수량·가격이 유효하지 않음
    INSUFFICIENT_HOLDING,  // 매도 가능 수량(보유 - 이미 예약된 매도) 초과
    INVALID_REQUEST_ID     // 재전송 멱등키(requestId) 누락
}
