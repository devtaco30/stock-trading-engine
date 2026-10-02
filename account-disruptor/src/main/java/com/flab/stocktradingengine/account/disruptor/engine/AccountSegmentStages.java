package com.flab.stocktradingengine.account.disruptor.engine;

import com.flab.stocktradingengine.time.LatencyHistogram;
import com.flab.stocktradingengine.time.StageLatencyRegistry;

/**
 * 구간 분해 측정(measure/segments 브랜치 한정)의 account-worker 단계 히스토그램 묶음.
 *
 * <p>승인된 주문(매수·매도 예약이 accept된 이벤트)만 기록한다. 거부·중복 주문은 끝점① 지점에 닿지
 * 않으므로 같은 모집단 규칙을 따른다(설계서 5-3절). 시각은 이벤트 슬롯에 싣고 승인 시점(W-P7)에
 * 한 번에 기록한다 — 폴링·저널·업무 스레드는 시각만 찍고 히스토그램에는 손대지 않는다.</p>
 *
 * <ul>
 *   <li>{@code A} = W-P2 − W-P0 (api 발신부터 수신 폴링 스레드가 읽을 때까지)</li>
 *   <li>{@code B} = W-P7 − W-P2 (설계서의 B′. 저널 기록을 포함한 접수 완료. 승인은 저널 기록 뒤에 일어난다)</li>
 *   <li>{@code B_biz} = W-P7 − W-P6 (업무 핸들러가 이벤트를 받아 판정에 도달할 때까지)</li>
 *   <li>{@code ring_publish} = W-P3 − W-P2 (링버퍼 슬롯 확보)</li>
 *   <li>{@code hop_to_journal} = W-P4 − W-P3 (슬롯 발행부터 저널 핸들러 스레드가 받을 때까지)</li>
 *   <li>{@code journal_append} = W-P5 − W-P4 (저널 기록)</li>
 *   <li>{@code hop_to_business} = W-P6 − W-P5 (저널 완료부터 업무 핸들러 스레드가 받을 때까지)</li>
 *   <li>체결 반영(매수 계좌 이벤트에서만): {@code E} = W-P20 − W-P14, {@code F} = W-P20 − aggressor의 W-P0,
 *       {@code fill_transit} = W-P17 − W-P14, {@code fill_hop_to_journal}, {@code fill_journal_append},
 *       {@code fill_hop_to_business}</li>
 * </ul>
 */
public final class AccountSegmentStages {

    public static final AccountSegmentStages DISABLED = new AccountSegmentStages(new StageLatencyRegistry(false));

    private final boolean enabled;
    private final LatencyHistogram a;
    private final LatencyHistogram b;
    private final LatencyHistogram bBiz;
    private final LatencyHistogram ringPublish;
    private final LatencyHistogram hopToJournal;
    private final LatencyHistogram journalAppend;
    private final LatencyHistogram hopToBusiness;
    private final LatencyHistogram e;
    private final LatencyHistogram f;
    private final LatencyHistogram fillTransit;
    private final LatencyHistogram fillHopToJournal;
    private final LatencyHistogram fillJournalAppend;
    private final LatencyHistogram fillHopToBusiness;

    public AccountSegmentStages(StageLatencyRegistry registry) {
        this.enabled = registry.isEnabled();
        this.a = registry.stage("A");
        this.b = registry.stage("B");
        this.bBiz = registry.stage("B_biz");
        this.ringPublish = registry.stage("ring_publish");
        this.hopToJournal = registry.stage("hop_to_journal");
        this.journalAppend = registry.stage("journal_append");
        this.hopToBusiness = registry.stage("hop_to_business");
        this.e = registry.stage("E");
        this.f = registry.stage("F");
        this.fillTransit = registry.stage("fill_transit");
        this.fillHopToJournal = registry.stage("fill_hop_to_journal");
        this.fillJournalAppend = registry.stage("fill_journal_append");
        this.fillHopToBusiness = registry.stage("fill_hop_to_business");
    }

    public boolean isEnabled() {
        return enabled;
    }

    /** 승인 시점(W-P7)에 이벤트 슬롯에 실린 시각으로 일곱 단계를 기록한다. 시각이 비어 있으면(복구 replay 등) 건너뛴다. */
    public void recordAccepted(AccountEvent event, long acceptedAtNanos) {
        long p0 = event.getPublishedAtEpochNanos();
        long p2 = event.getReceivedAtNanos();
        long p3 = event.getRingClaimedAtNanos();
        long p4 = event.getJournalStartedAtNanos();
        long p5 = event.getJournalEndedAtNanos();
        long p6 = event.getBusinessStartedAtNanos();
        if (p0 <= 0 || p2 <= 0 || p3 <= 0 || p4 <= 0 || p5 <= 0 || p6 <= 0) {
            return;
        }
        a.recordNanos(p2 - p0);
        b.recordNanos(acceptedAtNanos - p2);
        bBiz.recordNanos(acceptedAtNanos - p6);
        ringPublish.recordNanos(p3 - p2);
        hopToJournal.recordNanos(p4 - p3);
        journalAppend.recordNanos(p5 - p4);
        hopToBusiness.recordNanos(p6 - p5);
    }

    /**
     * 체결 반영 완료 시점(W-P20)에 체결 한 건의 E, F와 보조 단계를 기록한다. 체결 한 건은 매수 계좌
     * 이벤트에서만 부르므로(표본 한 건) 매도 쪽에서는 호출하지 않는다. 방출 시각이나 aggressor 발신 시각이
     * 비어 있으면(첫 체결이 아닌 체결, 복구 replay) 건너뛴다.
     */
    public void recordFillApplied(AccountEvent event, long appliedAtNanos) {
        long p0 = event.getAggressorOriginNanos();
        long p14 = event.getEmittedAtNanos();
        long p17 = event.getReceivedAtNanos();
        long p18 = event.getJournalStartedAtNanos();
        long p18End = event.getJournalEndedAtNanos();
        long p19 = event.getBusinessStartedAtNanos();
        if (p0 <= 0 || p14 <= 0 || p17 <= 0 || p18 <= 0 || p18End <= 0 || p19 <= 0) {
            return;
        }
        e.recordNanos(appliedAtNanos - p14);
        f.recordNanos(appliedAtNanos - p0);
        fillTransit.recordNanos(p17 - p14);
        fillHopToJournal.recordNanos(p18 - p17);
        fillJournalAppend.recordNanos(p18End - p18);
        fillHopToBusiness.recordNanos(p19 - p18End);
    }
}
