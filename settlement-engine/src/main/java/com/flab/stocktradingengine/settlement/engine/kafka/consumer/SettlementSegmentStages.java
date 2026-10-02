package com.flab.stocktradingengine.settlement.engine.kafka.consumer;

import com.flab.stocktradingengine.kafka.event.TradeFilledEvent;
import com.flab.stocktradingengine.time.LatencyHistogram;
import com.flab.stocktradingengine.time.StageLatencyRegistry;

/**
 * 구간 분해 측정(measure/segments 브랜치 한정)의 v1 settlement-engine 단계 히스토그램 묶음. 체결 한 건은
 * 정산 반영(DB 트랜잭션 하나로 매수·매도 주문과 계좌를 갱신)이 끝난 뒤 표본 한 건으로 기록한다 — v2가
 * 매수 체결 이벤트에서만 기록하는 것과 같은 모집단이다.
 *
 * <ul>
 *   <li>{@code fill_transit} = V-P11 − V-P10 (체결 방출부터 정산 컨슈머 리스너 진입까지)</li>
 *   <li>{@code E} = V-P12 − V-P10 (체결 방출부터 정산 반영 완료까지)</li>
 *   <li>{@code F} = V-P12 − V-P0 (체결을 일으킨 주문의 api 요청부터 정산 반영 완료까지)</li>
 * </ul>
 * 방출 시각이 비어 있으면(첫 체결이 아닌 체결, 옛 메시지) 기록하지 않는다.
 */
final class SettlementSegmentStages {

    static final SettlementSegmentStages DISABLED = new SettlementSegmentStages(new StageLatencyRegistry(false));

    private final boolean enabled;
    private final LatencyHistogram fillTransit;
    private final LatencyHistogram e;
    private final LatencyHistogram f;

    SettlementSegmentStages(StageLatencyRegistry registry) {
        this.enabled = registry.isEnabled();
        this.fillTransit = registry.stage("fill_transit");
        this.e = registry.stage("E");
        this.f = registry.stage("F");
    }

    boolean isEnabled() {
        return enabled;
    }

    void recordApplied(TradeFilledEvent fill, long receivedAtNanos, long appliedAtNanos) {
        if (fill.emittedAtNanos() <= 0 || fill.aggressorOriginNanos() <= 0) {
            return;
        }
        fillTransit.recordNanos(receivedAtNanos - fill.emittedAtNanos());
        e.recordNanos(appliedAtNanos - fill.emittedAtNanos());
        f.recordNanos(appliedAtNanos - fill.aggressorOriginNanos());
    }
}
