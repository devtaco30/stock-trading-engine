package com.flab.stocktradingengine.matching.worker.lifecycle;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.SmartLifecycle;

import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.worker.recovery.MatchingSnapshotStore;

/**
 * graceful stop 시점에 매칭 엔진 스냅샷을 찍어 저장한다(2d-1b, ADR-019 "자체 스냅샷").
 *
 * <h3>왜 {@link MatchingEngineLifecycle}과 분리했는가</h3>
 * <p>엔진 시작·종료 자체와 "종료 후에 스냅샷을 찍는다"는 서로 다른 관심사다. 나눠두면
 * {@code MatchingEngineConfigTest}처럼 Aeron Archive 없이 엔진만 가볍게 띄우는 테스트가 이
 * 클래스(AeronArchive 필요)를 몰라도 된다.</p>
 *
 * <h3>순서 — {@link MatchingEngineLifecycle}(phase 0)보다 낮은 phase</h3>
 * <p>SmartLifecycle은 낮은 phase부터 시작해 높은 phase부터(역순으로) 멈춘다. 이 빈이 phase -1이면
 * 엔진(phase 0)·수신 스레드(phase 1)가 먼저 멈춘 뒤에야 이 빈이 멈춘다 — {@link MatchingEngine#shutdown}
 * (Disruptor drain)이 끝나 소비자 스레드가 quiescent 상태가 된 다음에만 다른 스레드(이 stop() 호출
 * 스레드)가 books 를 안전하게 읽을 수 있어서다. 크래시(ungraceful)면 스냅샷을 못 찍고 직전
 * 스냅샷 + 그 뒤 저널 replay로 복구한다 — 그 공백을 줄이는 게 I6(주기 스냅샷)다.</p>
 *
 * <h3>recordingId — 더 이상 이 클래스가 직접 조회하지 않는다(I6 U2)</h3>
 * <p>이전엔 stop() 때마다 카탈로그를 스캔했다. 러닝 중 스냅샷 쓰기({@code MatchingSnapshotWriter})도
 * 같은 recordingId가 필요해지면서, {@code MatchingJournalArchiveConfig#matchingJournalRecordingId}
 * 빈으로 한 번만 조회해 공유한다(account-worker {@code AccountSnapshotLifecycle}과 같은 구조).</p>
 */
public class MatchingSnapshotLifecycle implements SmartLifecycle {

    private static final int PHASE = -1; // MatchingEngineLifecycle(phase 0)보다 늦게 멈춘다

    private final MatchingEngine engine;
    private final MatchingSnapshotStore snapshotStore;
    private final long journalRecordingId;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public MatchingSnapshotLifecycle(MatchingEngine engine, MatchingSnapshotStore snapshotStore, Long journalRecordingId) {
        this.engine = engine;
        this.snapshotStore = snapshotStore;
        this.journalRecordingId = journalRecordingId;
    }

    @Override
    public void start() {
        running.set(true);
    }

    @Override
    public void stop() {
        snapshotStore.write(journalRecordingId, engine.lastAppliedOrderIntakePositions(), engine.snapshot());
        running.set(false);
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return PHASE;
    }
}
