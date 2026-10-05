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
 *   <li>{@code M} = V-P10m − V-P0 (주문 발행부터 체결 완료까지. V-P10m은 {@code book.match()}가 체결 결과를 돌려준 직후이고
 *       로그, 최근 체결가 Redis 저장, tradeId 발급보다 앞이다. 체결을 일으킨 주문의 첫 체결만)</li>
 *   <li>{@code D_match} = V-P10m − V-P9 (매칭 계산만. v2의 {@code D}와 같은 정의이고 기존 {@code D}는 Redis 저장을 포함한다)</li>
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
    private final LatencyHistogram m;
    private final LatencyHistogram dMatch;

    MatchingSegmentStages(StageLatencyRegistry registry) {
        this.enabled = registry.isEnabled();
        this.c = registry.stage("C");
        this.d = registry.stage("D");
        this.aAggr = registry.stage("A@aggr");
        this.bAggr = registry.stage("B@aggr");
        this.cAggr = registry.stage("C@aggr");
        this.m = registry.stage("M");
        this.dMatch = registry.stage("D_match");
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

    void recordFirstFill(OrderPlacedEvent aggressor, long matchReceivedAtNanos, long matchedAtNanos, long emittedAtNanos) {
        if (!hasAcceptedTimes(aggressor)) {
            return;
        }
        long origin = EpochNanos.of(aggressor.orderAt());
        d.recordNanos(emittedAtNanos - matchReceivedAtNanos);
        dMatch.recordNanos(matchedAtNanos - matchReceivedAtNanos);
        m.recordNanos(matchedAtNanos - origin);
        aAggr.recordNanos(aggressor.receivedAtNanos() - origin);
        bAggr.recordNanos(aggressor.acceptedAtNanos() - aggressor.receivedAtNanos());
        cAggr.recordNanos(matchReceivedAtNanos - aggressor.acceptedAtNanos());
    }
}
