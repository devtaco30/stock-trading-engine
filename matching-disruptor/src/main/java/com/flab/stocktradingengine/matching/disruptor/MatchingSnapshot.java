package com.flab.stocktradingengine.matching.disruptor;

import java.util.Map;

/**
 * 매칭 엔진 전체 상태의 스냅샷(2d-1, ADR-019 "자체 스냅샷"). {@link MatchingEngine#snapshot()}이
 * 찍고 {@link MatchingEngine#restore(MatchingSnapshot)}가 되살린다.
 *
 * <p>{@code journalPosition}은 이 스냅샷을 찍은 시점의 저널 위치({@link Journal#position()})다 —
 * 복구할 때 저널을 처음부터가 아니라 이 위치부터만 리플레이하는 근거가 된다(2d-1b).</p>
 */
public record MatchingSnapshot(
    Map<String, BookSnapshot> booksByStock,
    long journalPosition
) {
}
