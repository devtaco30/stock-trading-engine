package com.flab.stocktradingengine.account.worker.recovery;

import com.flab.stocktradingengine.account.disruptor.snapshot.AccountSnapshot;

/**
 * 파일에서 읽은 스냅샷과, 그 스냅샷을 찍을 당시 저널 스트림의 recordingId(2d-2b) +
 * 체결(fill) 스트림의 소비 position(ADR-032, U4a). 둘 다 account-disruptor(엔진)가 모르는
 * host(Archive) 개념이라 이 모듈에 둔다 — {@code AccountSnapshot} 자체엔 안 담는다. matching
 * {@code StoredMatchingSnapshot}과 같은 결.
 *
 * <p>{@code fillConsumedPosition}은 {@code AccountFillReceiver.consumedPosition()}이 graceful
 * shutdown(quiescent) 시점에 소비한 체결 스트림(6001) 위치다 — 재기동 시 이 위치부터 fill
 * 스트림을 replay해야 그 사이 놓친 체결을 복구한다(U4b).</p>
 */
public record StoredAccountSnapshot(long recordingId, long fillConsumedPosition, AccountSnapshot snapshot) {
}
