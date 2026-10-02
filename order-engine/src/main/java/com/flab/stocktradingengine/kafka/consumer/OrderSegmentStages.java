package com.flab.stocktradingengine.kafka.consumer;

import com.flab.stocktradingengine.time.LatencyHistogram;
import com.flab.stocktradingengine.time.StageLatencyRegistry;
import com.flab.stocktradingengine.trading.service.OrderStageTimes;

/**
 * 구간 분해 측정(measure/segments 브랜치 한정)의 v1 order-engine 단계 히스토그램 묶음. 접수 완료(DB 커밋까지
 * 끝난) 주문만 기록한다 — 거부와 이미 접수된 요청은 저장 지점(V-P6)이 비어 있어 건너뛴다.
 *
 * <ul>
 *   <li>{@code A} = V-P1 − V-P0 (api 요청부터 컨슈머 리스너 진입까지, Kafka 전달과 역직렬화 포함)</li>
 *   <li>{@code b_idem} = V-P3 − V-P1, {@code b_lock} = V-P4 − V-P3, {@code b_sum} = V-P5 − V-P4,
 *       {@code b_insert} = V-P6 − V-P5</li>
 *   <li>{@code b_decide} = V-P5b − V-P4 (판정 도달. v2의 {@code B_biz}에 대응)</li>
 *   <li>{@code B} = V-P7 − V-P1 (접수 완료, 커밋 포함. v2의 {@code B}에 대응)</li>
 *   <li>{@code b_commit} = V-P7 − V-P6 (영속화 비용. v2의 {@code journal_append}에 대응)</li>
 *   <li>{@code b_send_call} = V-P8 − V-P7 (orders 토픽 send 호출)</li>
 * </ul>
 */
final class OrderSegmentStages {

    static final OrderSegmentStages DISABLED = new OrderSegmentStages(new StageLatencyRegistry(false));

    private final boolean enabled;
    private final LatencyHistogram a;
    private final LatencyHistogram bIdem;
    private final LatencyHistogram bLock;
    private final LatencyHistogram bSum;
    private final LatencyHistogram bDecide;
    private final LatencyHistogram bInsert;
    private final LatencyHistogram b;
    private final LatencyHistogram bCommit;
    private final LatencyHistogram bSendCall;

    OrderSegmentStages(StageLatencyRegistry registry) {
        this.enabled = registry.isEnabled();
        this.a = registry.stage("A");
        this.bIdem = registry.stage("b_idem");
        this.bLock = registry.stage("b_lock");
        this.bSum = registry.stage("b_sum");
        this.bDecide = registry.stage("b_decide");
        this.bInsert = registry.stage("b_insert");
        this.b = registry.stage("B");
        this.bCommit = registry.stage("b_commit");
        this.bSendCall = registry.stage("b_send_call");
    }

    boolean isEnabled() {
        return enabled;
    }

    /** 저장까지 간 주문인지(거부·이미 접수된 요청이면 false). */
    static boolean wasInserted(OrderStageTimes times) {
        return times.insertedAtNanos() > 0;
    }

    void recordAccepted(long originNanos, OrderStageTimes times, long acceptedAtNanos, long sentAtNanos) {
        long p1 = times.receivedAtNanos();
        a.recordNanos(p1 - originNanos);
        bIdem.recordNanos(times.idemCheckedAtNanos() - p1);
        bLock.recordNanos(times.lockedAtNanos() - times.idemCheckedAtNanos());
        bSum.recordNanos(times.summedAtNanos() - times.lockedAtNanos());
        bDecide.recordNanos(times.decidedAtNanos() - times.lockedAtNanos());
        bInsert.recordNanos(times.insertedAtNanos() - times.summedAtNanos());
        b.recordNanos(acceptedAtNanos - p1);
        bCommit.recordNanos(acceptedAtNanos - times.insertedAtNanos());
        bSendCall.recordNanos(sentAtNanos - acceptedAtNanos);
    }
}
