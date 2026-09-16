package com.flab.stocktradingengine.matching.worker.recovery;

import java.util.Map;

import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshot;

/**
 * 파일에서 읽은 스냅샷과, 그 스냅샷을 찍을 당시 저널 스트림의 recordingId(2d-1b) + 주문 인테이크
 * 스트림의 소비 position(I2 U1). 셋 다 matching-disruptor(엔진)가 모르는 host(Archive) 개념이라
 * 이 모듈에 둔다 — {@code MatchingSnapshot} 자체엔 안 담는다. account
 * {@code StoredAccountSnapshot}과 같은 결.
 *
 * <p>{@code orderIntakePosition}은 {@code MatchingEngine.lastAppliedOrderIntakePositions()}가
 * graceful shutdown(quiescent) 시점에 소비자 스레드가 실제로 반영한 주문 인테이크 스트림을
 * 발행자(Aeron sessionId=계좌 샤드)별로 담은 맵이다 — 재기동 시 이 위치들부터 각 발행자의
 * 인테이크 스트림을 replay해야 그 사이 놓친 주문을 복구한다(I2 U2b).</p>
 */
public record StoredMatchingSnapshot(long recordingId, Map<Integer, Long> orderIntakePosition, MatchingSnapshot snapshot) {
}
