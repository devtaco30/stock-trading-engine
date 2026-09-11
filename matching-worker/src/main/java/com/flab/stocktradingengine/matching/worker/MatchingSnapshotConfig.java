package com.flab.stocktradingengine.matching.worker;

import java.io.File;
import java.util.Optional;

import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.matching.disruptor.MatchingEngine;

import io.aeron.archive.client.AeronArchive;

/**
 * 매칭 엔진 스냅샷(2d-1b) 배선. {@link MatchingOrderIntakeConfig}가 만든 archive-dir(저널과 같은
 * durable 디스크)에 스냅샷 파일을 두고, graceful stop 때({@link MatchingSnapshotLifecycle}) 쓰고
 * 기동 때({@link #matchingLoadedSnapshot}) 읽는다.
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
            MatchingEngine matchingEngine, MatchingSnapshotStore matchingSnapshotStore, AeronArchive aeronArchive) {
        return new MatchingSnapshotLifecycle(matchingEngine, matchingSnapshotStore, aeronArchive);
    }
}
