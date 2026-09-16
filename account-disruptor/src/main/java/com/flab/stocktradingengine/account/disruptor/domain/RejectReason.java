package com.flab.stocktradingengine.account.disruptor.domain;

/** 매수·매도 검증 거부 사유. */
public enum RejectReason {
    NOT_OWNED,             // 이 워커가 맡은 슬롯이 아님(라우팅이 잘못 왔거나 설정이 어긋남, I8 U2)
    ACCOUNT_NOT_FOUND,     // 담당 슬롯이지만 그 계좌가 메모리에 없음(시드 누락 등)
    INSUFFICIENT,          // 매수 가능 금액 초과
    INVALID_QUANTITY,      // 수량·가격이 유효하지 않음
    INSUFFICIENT_HOLDING,  // 매도 가능 수량(보유 - 이미 예약된 매도) 초과
    INVALID_REQUEST_ID     // 재전송 멱등키(requestId) 누락
}
