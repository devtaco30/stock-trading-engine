package com.flab.stocktradingengine.matching.worker.config;

import java.io.File;
import java.util.Optional;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
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

    /**
     * {@code matching.worker.graceful-snapshot-enabled=false}면 이 빈 자체가 등록되지 않는다(I6
     * U3) — graceful stop 때 찍는 최종 스냅샷을 완전히 빼서, "정상 종료를 거치지 않고 러닝 중
     * 스냅샷만으로 복구되는지"를 증명하는 통합 테스트가 쓴다. 프로덕션 기본값은 true(항상 켜짐) —
     * 이 스위치를 끄는 건 그 테스트 하나뿐이다.
     *
     * <h3>⚠️ 운영에서는 끄지 말 것</h3>
     * <p>테스트에서 비정상 종료(크래시)를 흉내내려고 둔 스위치다. 이 값을 false로 두고 실제로
     * 운영하면, 프로세스가 정상 종료할 때도 그 시점의 최종 스냅샷이 남지 않는다 — 다음 기동이
     * 마지막 러닝 중 스냅샷(최대 10,000건 전) 이후 구간을 전부 저널에서 다시 읽어야 해 복구가
     * 그만큼 길어진다. 기본값이 true라 실수로 꺼질 일은 없지만, "종료를 빠르게 하려고" 끄는
     * 용도로 오해하지 말 것 — 이건 종료 속도를 위한 스위치가 아니라 테스트 전용 스위치다.
     */
    @Bean
    @ConditionalOnProperty(name = "matching.worker.graceful-snapshot-enabled", havingValue = "true", matchIfMissing = true)
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
