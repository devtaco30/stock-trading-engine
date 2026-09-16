package com.flab.stocktradingengine.matching.worker.config;

import java.io.File;
import java.util.Optional;

import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.worker.lifecycle.MatchingSnapshotLifecycle;
import com.flab.stocktradingengine.matching.worker.lifecycle.MatchingSnapshotWriterLifecycle;
import com.flab.stocktradingengine.matching.worker.recovery.MatchingSnapshotStore;
import com.flab.stocktradingengine.matching.worker.recovery.MatchingSnapshotWriter;
import com.flab.stocktradingengine.matching.worker.recovery.StoredMatchingSnapshot;

/**
 * 매칭 엔진 스냅샷(2d-1b) 배선. {@link MatchingOrderIntakeConfig}가 만든 archive-dir(저널과 같은
 * durable 디스크)에 스냅샷 파일을 두고, graceful stop 때({@link MatchingSnapshotLifecycle}) 쓰고
 * 기동 때({@link #matchingLoadedSnapshot}) 읽는다. account-worker {@code AccountSnapshotConfig}와
 * 같은 결.
 *
 * <p>{@link #matchingLoadedSnapshot}은 Archive 녹화를 건드리지 않는 순수 파일 읽기라
 * {@link MatchingJournalArchiveConfig}의 "과거 읽기 → 새 녹화 시작" 순서 제약과 무관하다 — 다만
 * {@link MatchingJournalArchiveConfig#matchingJournalRecoveredEntries}가 이 빈에 의존해 스냅샷
 * 위치부터만 읽을지 처음부터 다 읽을지 결정한다.</p>
 */
@Configuration
public class MatchingSnapshotConfig {

    @Bean
    public MatchingSnapshotStore matchingSnapshotStore(File matchingArchiveDir) {
        return new MatchingSnapshotStore(matchingArchiveDir);
    }

    @Bean
    public Optional<StoredMatchingSnapshot> matchingLoadedSnapshot(MatchingSnapshotStore matchingSnapshotStore) {
        return matchingSnapshotStore.read();
    }

    @Bean
    public SmartLifecycle matchingSnapshotLifecycle(
            MatchingEngine matchingEngine, MatchingSnapshotStore matchingSnapshotStore, Long matchingJournalRecordingId) {
        return new MatchingSnapshotLifecycle(matchingEngine, matchingSnapshotStore, matchingJournalRecordingId);
    }

    /**
     * 러닝 중 스냅샷 쓰기 스레드(I6 U2) — {@code MatchingEngineConfig#matchingEngine}이 이 빈을
     * {@code MatchingSnapshotSink}로 넘겨 소비자 스레드가 만든 스냅샷 바이트를 받는다. 시작·종료는
     * {@link MatchingSnapshotWriterLifecycle}이 맡는다(별도 SmartLifecycle — 엔진 소비자 스레드와
     * 무관하게 이 쓰기 스레드는 엔진 시작 전부터 대기해도 안전하다, offer만 받고 실제 쓰기는
     * start() 이후에나 일어나므로).
     */
    @Bean
    public MatchingSnapshotWriter matchingSnapshotWriter(MatchingSnapshotStore matchingSnapshotStore, Long matchingJournalRecordingId) {
        return new MatchingSnapshotWriter(matchingSnapshotStore, matchingJournalRecordingId);
    }

    @Bean
    public SmartLifecycle matchingSnapshotWriterLifecycle(MatchingSnapshotWriter matchingSnapshotWriter) {
        return new MatchingSnapshotWriterLifecycle(matchingSnapshotWriter);
    }
}
