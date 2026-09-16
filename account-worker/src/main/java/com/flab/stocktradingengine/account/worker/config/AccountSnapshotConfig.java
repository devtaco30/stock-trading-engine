package com.flab.stocktradingengine.account.worker.config;

import java.io.File;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.worker.lifecycle.AccountSnapshotLifecycle;
import com.flab.stocktradingengine.account.worker.lifecycle.AccountSnapshotWriterLifecycle;
import com.flab.stocktradingengine.account.worker.recovery.AccountArchiveSegmentPurger;
import com.flab.stocktradingengine.account.worker.recovery.AccountSnapshotStore;
import com.flab.stocktradingengine.account.worker.recovery.AccountSnapshotWriter;
import com.flab.stocktradingengine.account.worker.recovery.StoredAccountSnapshot;
import com.flab.stocktradingengine.aeron.AeronStreamIds;

import io.aeron.Aeron;

/**
 * 계좌 엔진 스냅샷(2d-2b) 배선. {@link AccountOrderIntakeConfig}가 만든 archive-dir(저널과 같은
 * durable 디스크)에 스냅샷 파일을 두고, graceful stop 때({@link AccountSnapshotLifecycle}) 쓰고
 * 기동 때({@link #accountLoadedSnapshot}) 읽는다. matching {@code MatchingSnapshotConfig}와 같은 결.
 *
 * <p>{@link #accountLoadedSnapshot}은 Archive 녹화를 건드리지 않는 순수 파일 읽기라
 * {@link AccountJournalArchiveConfig}의 "과거 읽기 → 새 녹화 시작" 순서 제약과 무관하다 — 다만
 * {@link AccountJournalArchiveConfig#accountJournalRecoveredEntries}가 이 빈에 의존해 스냅샷
 * 위치부터만 읽을지 처음부터 다 읽을지 결정한다.</p>
 */
@Configuration
public class AccountSnapshotConfig {

    // AccountOrderIntakeConfig.aeronArchive가 쓰는 것과 같은 property·기본값 — 회수 전용 연결이
    // 같은 Archive를 가리켜야 하므로(같은 제어 채널) 이 값을 그대로 따른다.
    private static final String DEFAULT_CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8010";

    @Bean
    public AccountArchiveSegmentPurger accountArchiveSegmentPurger(
            Aeron aeron, Long accountJournalRecordingId,
            @Value("${account.worker.archive.control-channel:" + DEFAULT_CONTROL_REQUEST_CHANNEL + "}") String controlRequestChannel,
            @Value("${transport.fill.channel:" + AccountFillIntakeConfig.DEFAULT_FILL_CHANNEL + "}") String fillChannel) {
        return new AccountArchiveSegmentPurger(aeron, controlRequestChannel, accountJournalRecordingId,
            fillChannel, AeronStreamIds.FILL);
    }

    @Bean
    public AccountSnapshotStore accountSnapshotStore(File accountArchiveDir) {
        return new AccountSnapshotStore(accountArchiveDir);
    }

    @Bean
    public Optional<StoredAccountSnapshot> accountLoadedSnapshot(AccountSnapshotStore accountSnapshotStore) {
        return accountSnapshotStore.read();
    }

    @Bean
    public SmartLifecycle accountSnapshotLifecycle(AccountEngine accountEngine,
            AccountSnapshotStore accountSnapshotStore, Long accountJournalRecordingId) {
        return new AccountSnapshotLifecycle(accountEngine, accountSnapshotStore, accountJournalRecordingId);
    }

    /**
     * 러닝 중 스냅샷 쓰기 스레드(1-3) — {@code AccountEngineConfig#accountEngine}이 이 빈을
     * {@code AccountSnapshotSink}로 넘겨 소비자 스레드가 만든 스냅샷 바이트를 받는다. 시작·종료는
     * {@link AccountSnapshotWriterLifecycle}이 맡는다(별도 SmartLifecycle — 엔진 소비자 스레드와
     * 무관하게 이 쓰기 스레드는 엔진 시작 전부터 대기해도 안전하다, offer만 받고 실제 쓰기는
     * start() 이후에나 일어나므로).
     */
    @Bean
    public AccountSnapshotWriter accountSnapshotWriter(AccountSnapshotStore accountSnapshotStore,
            Long accountJournalRecordingId, AccountArchiveSegmentPurger accountArchiveSegmentPurger) {
        return new AccountSnapshotWriter(accountSnapshotStore, accountJournalRecordingId, accountArchiveSegmentPurger);
    }

    @Bean
    public SmartLifecycle accountSnapshotWriterLifecycle(AccountSnapshotWriter accountSnapshotWriter) {
        return new AccountSnapshotWriterLifecycle(accountSnapshotWriter);
    }
}
