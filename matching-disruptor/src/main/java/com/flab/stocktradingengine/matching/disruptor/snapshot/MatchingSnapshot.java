package com.flab.stocktradingengine.matching.disruptor.snapshot;

import java.util.Map;

import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.journal.Journal;

/**
 * 매칭 엔진 전체 상태의 스냅샷(2d-1, ADR-019 "자체 스냅샷"). {@link MatchingEngine#snapshot()}이
 * 찍고 {@link MatchingEngine#restore(MatchingSnapshot)}가 되살린다.
 *
 * <p>{@code journalPosition}은 이 스냅샷을 찍은 시점의 저널 위치({@link Journal#position()})다 —
 * 복구할 때 저널을 처음부터가 아니라 이 위치부터만 리플레이하는 근거가 된다(2d-1b).</p>
 *
 * <p>{@code tradeIdCounter}는 {@code MatchingTradeIdGenerator}가 지금까지 발급한 횟수다. 복원한 엔진이 그다음 번호부터
 * 이어 발급하는 근거가 된다(ADR-023 갱신 2c).</p>
 */
public record MatchingSnapshot(
    Map<String, BookSnapshot> booksByStock,
    long journalPosition,
    long tradeIdCounter
) {

    /** 체결번호 카운터가 없던 시절의 스냅샷(카운터 0). 옛 형식 파일을 읽을 때와 카운터가 필요 없는 테스트가 쓴다. */
    public MatchingSnapshot(Map<String, BookSnapshot> booksByStock, long journalPosition) {
        this(booksByStock, journalPosition, 0L);
    }
}
