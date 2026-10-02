package com.flab.stocktradingengine.matching.kafka.consumer;

import com.flab.stocktradingengine.kafka.event.OrderPlacedEvent;
import com.flab.stocktradingengine.time.EpochNanos;
import com.flab.stocktradingengine.time.LatencyHistogram;
import com.flab.stocktradingengine.time.StageLatencyRegistry;

/**
 * 구간 분해 측정(measure/segments 브랜치 한정)의 v1 matching-engine 단계 히스토그램 묶음.
 *
 * <ul>
 *   <li>{@code C} = V-P9 − V-P7 (order-engine의 접수 완료부터 매칭 컨슈머 리스너 진입까지, 접수 완료된 주문마다)</li>
 *   <li>{@code D} = V-P10 − V-P9 (체결을 일으킨 주문의 첫 체결만. 호가창 매칭과 최근 체결가 Redis 저장 포함)</li>
 *   <li>{@code A@aggr} = V-P1 − V-P0, {@code B@aggr} = V-P7 − V-P1, {@code C@aggr} = V-P9 − V-P7 —
 *       같은 체결을 일으킨 주문 기준의 A, B, C. V-P0은 이벤트의 {@code orderAt}(api가 요청을 받은 시각)이다.</li>
 * </ul>
 * 접수 시각이 비어 있는 옛 메시지는 기록하지 않는다.
 */
final class MatchingSegmentStages {

    static final MatchingSegmentStages DISABLED = new MatchingSegmentStages(new StageLatencyRegistry(false));

    private final boolean enabled;
    private final LatencyHistogram c;
    private final LatencyHistogram d;
    private final LatencyHistogram aAggr;
    private final LatencyHistogram bAggr;
    private final LatencyHistogram cAggr;

    MatchingSegmentStages(StageLatencyRegistry registry) {
        this.enabled = registry.isEnabled();
        this.c = registry.stage("C");
        this.d = registry.stage("D");
        this.aAggr = registry.stage("A@aggr");
        this.bAggr = registry.stage("B@aggr");
        this.cAggr = registry.stage("C@aggr");
    }

    boolean isEnabled() {
        return enabled;
    }

    static boolean hasAcceptedTimes(OrderPlacedEvent event) {
        return event.receivedAtNanos() > 0 && event.acceptedAtNanos() > 0;
    }

    void recordArrival(OrderPlacedEvent event, long matchReceivedAtNanos) {
        if (hasAcceptedTimes(event)) {
            c.recordNanos(matchReceivedAtNanos - event.acceptedAtNanos());
        }
    }

    void recordFirstFill(OrderPlacedEvent aggressor, long matchReceivedAtNanos, long emittedAtNanos) {
        if (!hasAcceptedTimes(aggressor)) {
            return;
        }
        long origin = EpochNanos.of(aggressor.orderAt());
        d.recordNanos(emittedAtNanos - matchReceivedAtNanos);
        aAggr.recordNanos(aggressor.receivedAtNanos() - origin);
        bAggr.recordNanos(aggressor.acceptedAtNanos() - aggressor.receivedAtNanos());
        cAggr.recordNanos(matchReceivedAtNanos - aggressor.acceptedAtNanos());
    }
}
