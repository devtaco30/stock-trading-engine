package com.flab.stocktradingengine.matching.worker.recovery;

import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshot;

/**
 * 파일에서 읽은 스냅샷과, 그 스냅샷을 찍을 당시 저널 스트림의 recordingId(2d-1b). recordingId는
 * matching-disruptor(엔진)가 모르는 host(Archive) 개념이라 이 모듈에 둔다 — {@code MatchingSnapshot}
 * 자체엔 안 담는다.
 */
public record StoredMatchingSnapshot(long recordingId, MatchingSnapshot snapshot) {
}
