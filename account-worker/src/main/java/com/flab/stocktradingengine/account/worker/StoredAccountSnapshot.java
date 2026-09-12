package com.flab.stocktradingengine.account.worker;

import com.flab.stocktradingengine.account.disruptor.AccountSnapshot;

/**
 * 파일에서 읽은 스냅샷과, 그 스냅샷을 찍을 당시 저널 스트림의 recordingId(2d-2b). recordingId는
 * account-disruptor(엔진)가 모르는 host(Archive) 개념이라 이 모듈에 둔다 — {@code AccountSnapshot}
 * 자체엔 안 담는다. matching {@code StoredMatchingSnapshot}과 같은 결.
 */
record StoredAccountSnapshot(long recordingId, AccountSnapshot snapshot) {
}
