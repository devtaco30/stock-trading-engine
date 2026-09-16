package com.flab.stocktradingengine.matching.worker.lifecycle;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.SmartLifecycle;

import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.worker.config.MatchingJournalArchiveConfig;
import com.flab.stocktradingengine.matching.worker.recovery.MatchingSnapshotStore;

import io.aeron.archive.client.AeronArchive;

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
 * 스냅샷 + 그 뒤 저널 replay로 복구한다(메커니즘 먼저 — 주기적 라이브 스냅샷은 나중 리파인).</p>
 */
public class MatchingSnapshotLifecycle implements SmartLifecycle {

    private static final int PHASE = -1; // MatchingEngineLifecycle(phase 0)보다 늦게 멈춘다

    private final MatchingEngine engine;
    private final MatchingSnapshotStore snapshotStore;
    private final AeronArchive aeronArchive;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public MatchingSnapshotLifecycle(MatchingEngine engine, MatchingSnapshotStore snapshotStore, AeronArchive aeronArchive) {
        this.engine = engine;
        this.snapshotStore = snapshotStore;
        this.aeronArchive = aeronArchive;
    }

    @Override
    public void start() {
        running.set(true);
    }

    @Override
    public void stop() {
        long recordingId = resolveCurrentJournalRecordingId();
        snapshotStore.write(recordingId, engine.snapshot());
        running.set(false);
    }

    /** 저널 스트림의 현재(=이 프로세스가 기동 때 시작한) 녹화 ID — 카탈로그에서 가장 최근에 시작한 것. */
    private long resolveCurrentJournalRecordingId() {
        long[] latestRecordingId = {-1L};
        long[] latestStartTimestamp = {Long.MIN_VALUE};
        aeronArchive.listRecordingsForUri(0, 100,
            MatchingJournalArchiveConfig.JOURNAL_CHANNEL, MatchingJournalArchiveConfig.JOURNAL_STREAM_ID,
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
             startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
             mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
                if (startTimestamp > latestStartTimestamp[0]) {
                    latestStartTimestamp[0] = startTimestamp;
                    latestRecordingId[0] = recordingId;
                }
            });
        if (latestRecordingId[0] < 0) {
            throw new IllegalStateException("저널 스트림의 녹화를 카탈로그에서 찾지 못했습니다 — 스냅샷을 찍을 수 없습니다");
        }
        return latestRecordingId[0];
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
