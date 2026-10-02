package com.flab.stocktradingengine.matching.disruptor.engine;

import com.flab.stocktradingengine.time.LatencyHistogram;
import com.flab.stocktradingengine.time.StageLatencyRegistry;

/**
 * 구간 분해 측정(measure/segments 브랜치 한정)의 매칭 프로세스 단계 히스토그램 묶음. 모두 매칭 핸들러
 * 스레드가 기록한다(수신 폴링·링버퍼 발행 스레드는 시각만 찍는다).
 *
 * <ul>
 *   <li>{@code C} = W-P11 − W-P8 (계좌가 승인 후 전달한 시점부터 매칭 수신 폴링 스레드가 읽을 때까지, 접수된 주문마다)</li>
 *   <li>{@code m_ring_publish} = W-P12 − W-P11, {@code m_hop_to_business} = W-P13 − W-P12 (저널 핸들러를 거쳐 매칭 핸들러가 받을 때까지)</li>
 *   <li>{@code D} = W-P14 − W-P11 (체결을 일으킨 주문의 첫 체결만)</li>
 *   <li>{@code A@aggr}, {@code B@aggr}, {@code C@aggr} — 같은 체결을 일으킨 주문 기준의 A, B, C.
 *       A@aggr = W-P2 − W-P0, B@aggr = W-P8 − W-P2, C@aggr = W-P11 − W-P8. 체결 한 건의 F 항등식에 쓴다.</li>
 * </ul>
 */
public final class MatchingSegmentStages {

    public static final MatchingSegmentStages DISABLED = new MatchingSegmentStages(new StageLatencyRegistry(false));

    private final boolean enabled;
    private final LatencyHistogram c;
    private final LatencyHistogram ringPublish;
    private final LatencyHistogram hopToBusiness;
    private final LatencyHistogram d;
    private final LatencyHistogram aAggr;
    private final LatencyHistogram bAggr;
    private final LatencyHistogram cAggr;

    public MatchingSegmentStages(StageLatencyRegistry registry) {
        this.enabled = registry.isEnabled();
        this.c = registry.stage("C");
        this.ringPublish = registry.stage("m_ring_publish");
        this.hopToBusiness = registry.stage("m_hop_to_business");
        this.d = registry.stage("D");
        this.aAggr = registry.stage("A@aggr");
        this.bAggr = registry.stage("B@aggr");
        this.cAggr = registry.stage("C@aggr");
    }

    public boolean isEnabled() {
        return enabled;
    }

    /** 접수된 주문이 매칭 핸들러에 닿았을 때(W-P13) C와 매칭 쪽 보조 단계를 기록한다. 시각이 비어 있으면(복구 replay 등) 건너뛴다. */
    public void recordArrival(OrderEvent event, long businessStartedAtNanos) {
        long p8 = event.getAcceptedAtNanos();
        long p11 = event.getMatchReceivedAtNanos();
        long p12 = event.getRingClaimedAtNanos();
        if (p8 <= 0 || p11 <= 0 || p12 <= 0 || businessStartedAtNanos <= 0) {
            return;
        }
        c.recordNanos(p11 - p8);
        ringPublish.recordNanos(p12 - p11);
        hopToBusiness.recordNanos(businessStartedAtNanos - p12);
    }

    /** 체결을 일으킨 주문의 첫 체결(W-P14)에서 D와 aggressor 기준 A, B, C를 기록한다. */
    public void recordFirstFill(OrderEvent event, long emittedAtNanos) {
        long p0 = event.getOriginNanos();
        long p2 = event.getReceivedAtNanos();
        long p8 = event.getAcceptedAtNanos();
        long p11 = event.getMatchReceivedAtNanos();
        if (p0 <= 0 || p2 <= 0 || p8 <= 0 || p11 <= 0) {
            return;
        }
        d.recordNanos(emittedAtNanos - p11);
        aAggr.recordNanos(p2 - p0);
        bAggr.recordNanos(p8 - p2);
        cAggr.recordNanos(p11 - p8);
    }
}
