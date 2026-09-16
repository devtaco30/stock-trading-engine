package com.flab.stocktradingengine.account.worker.recovery;

import java.util.Map;

import com.flab.stocktradingengine.account.disruptor.snapshot.AccountSnapshot;

/**
 * 파일에서 읽은 스냅샷과, 그 스냅샷을 찍을 당시 저널 스트림의 recordingId(2d-2b) +
 * 체결(fill) 스트림의 소비 position(ADR-032, U4a·I1). 둘 다 account-disruptor(엔진)가 모르는
 * host(Archive) 개념이라 이 모듈에 둔다 — {@code AccountSnapshot} 자체엔 안 담는다. matching
 * {@code StoredMatchingSnapshot}과 같은 결.
 *
 * <p>{@code fillConsumedPosition}은 {@code AccountEngine.lastAppliedFillPositions()}가 graceful
 * shutdown(quiescent) 시점에 소비자 스레드가 실제로 반영한 체결 스트림(6001) 위치를 발행자
 * (Aeron sessionId, ADR-032 I1 D1)별로 담은 맵이다 — 재기동 시 이 위치들부터 각 발행자의 체결
 * 스트림을 replay해야 그 사이 놓친 체결을 복구한다(U4b, I1 U3).</p>
 */
public record StoredAccountSnapshot(long recordingId, Map<Integer, Long> fillConsumedPosition, AccountSnapshot snapshot) {
}
