package com.flab.stocktradingengine.account.worker.config;

import java.util.List;
import java.util.Optional;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.account.disruptor.journal.AeronArchiveAccountJournal;
import com.flab.stocktradingengine.account.worker.recovery.AccountJournalReplayer;
import com.flab.stocktradingengine.account.worker.recovery.StoredAccountSnapshot;
import com.flab.stocktradingengine.codec.AccountJournalEntry;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;

/**
 * 계좌 저널(2b-1)을 Aeron Archive로 durable 녹화하는 배선(2b-1b). {@link AccountOrderIntakeConfig}가
 * 만든 account-worker 자체 Aeron·Archive(ArchivingMediaDriver)를 그대로 쓴다.
 *
 * <h3>순서 (저널 유실·오염 방지)</h3>
 * <p>①{@link #accountJournalRecoveredEntries}(이전 녹화 읽기, 2b-2b) → ②
 * {@link #accountJournalRecordingSubscriptionId}(새 녹화 시작) → ③
 * {@link #accountJournalPublication}(발행 스트림) 순서로 만들어지도록 각자 앞 단계 빈을 파라미터로
 * 받아 의존시킨다. ①이 ②보다 먼저여야 하는 이유는, ②가 시작되기 전엔 이번 실행의 새 녹화가
 * 카탈로그에 아직 없어(Archive는 이미지가 실제로 붙어야 recordingId를 만든다) 리플레이가 과거
 * 녹화만 읽는다는 게 보장되기 때문이다 — 순서가 바뀌면 리플레이가 이번 실행 자신의(아직 비어있는)
 * 녹화까지 스캔 대상에 넣을 여지가 생긴다. ②가 ③보다 먼저여야 하는 이유는 2a와 같다(IPC에서 새
 * 구독자는 붙은 시점 이후 데이터만 보므로, 녹화가 발행보다 늦게 시작되면 그 사이 엔트리가 빠진다).</p>
 */
@Configuration
public class AccountJournalArchiveConfig {

    // AccountSnapshotLifecycle(lifecycle 패키지)·테스트가 프로덕션과 같은 채널·스트림을
    // 그대로 참조해야 해서 public이다.
    public static final String JOURNAL_CHANNEL = "aeron:ipc";
    public static final int JOURNAL_STREAM_ID = 4005; // 인테이크(4004)·매칭 인테이크(2002)와 구분되는 저널 전용 스트림

    /**
     * 이 스트림에서 복구 입력을 읽어 만든다.
     * {@link com.flab.stocktradingengine.account.disruptor.engine.AccountEngine#recover}에 그대로 넘긴다
     * ({@code AccountEngineConfig} 참고). 반드시 {@link #accountJournalRecordingSubscriptionId}보다
     * 먼저 만들어져야 한다(클래스 javadoc 순서 참고) — 이 빈 자체가 그 순서를 강제한다.
     *
     * <p>스냅샷이 있으면(2d-2b) 스냅샷이 가리키는 recordingId·position부터만 읽는다 — 스냅샷이
     * 이미 그 앞까지의 상태를 담고 있어 처음부터 다시 읽을 이유가 없다. 스냅샷이 없으면(2b-2b,
     * 하위호환) 이전 녹화 전부를 처음부터 읽는다.</p>
     */
    @Bean
    public List<AccountJournalEntry> accountJournalRecoveredEntries(
            AeronArchive aeronArchive, Optional<StoredAccountSnapshot> accountLoadedSnapshot) {
        AccountJournalReplayer replayer = new AccountJournalReplayer(aeronArchive);
        if (accountLoadedSnapshot.isPresent()) {
            StoredAccountSnapshot stored = accountLoadedSnapshot.get();
            return replayer.readFrom(JOURNAL_CHANNEL, JOURNAL_STREAM_ID, stored.recordingId(), stored.snapshot().journalPosition());
        }
        return replayer.readAll(JOURNAL_CHANNEL, JOURNAL_STREAM_ID);
    }

    /**
     * 저널 채널·스트림에 새 녹화를 시작한다. 반환값(Archive 구독 ID)은 안 쓴다 — 이 빈이 존재하는
     * 이유는 {@link #accountJournalPublication}이 이 빈에 의존하게 만들어 생성 순서를 강제하는
     * 것뿐이다(클래스 javadoc "순서" 참고). {@code accountJournalRecoveredEntries}를 파라미터로
     * 받는 이유도 같다 — 리플레이가 끝난 뒤에야 새 녹화를 시작해야 한다.
     */
    @Bean
    public Long accountJournalRecordingSubscriptionId(AeronArchive aeronArchive, List<AccountJournalEntry> accountJournalRecoveredEntries) {
        return aeronArchive.startRecording(JOURNAL_CHANNEL, JOURNAL_STREAM_ID, SourceLocation.LOCAL);
    }

    /**
     * 이 프로세스가 이번 실행에서 시작한 저널 녹화의 recordingId(1-3) — 카탈로그에서 시작 시각이
     * 가장 최근인 것. {@link #accountJournalRecordingSubscriptionId}에 의존해 그 녹화가 실제로
     * 시작된 뒤에만(=카탈로그에 나타난 뒤에만) 조회한다. 한 프로세스 실행 동안 이 스트림에 recording이
     * 정확히 하나뿐이라(재시작 없는 한 run) 기동 시 한 번만 조회해도 이후 값이 안 바뀐다 — graceful
     * stop 스냅샷({@link com.flab.stocktradingengine.account.worker.lifecycle.AccountSnapshotLifecycle})과
     * 러닝 중 스냅샷 쓰기({@code AccountSnapshotWriter}, 1-3) 둘 다 이 빈을 그대로 쓴다(이전엔 각자
     * 따로 카탈로그를 스캔했다).
     */
    @Bean
    public Long accountJournalRecordingId(AeronArchive aeronArchive, Long accountJournalRecordingSubscriptionId) {
        long[] latestRecordingId = {-1L};
        long[] latestStartTimestamp = {Long.MIN_VALUE};
        aeronArchive.listRecordingsForUri(0, 100, JOURNAL_CHANNEL, JOURNAL_STREAM_ID,
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
             startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
             mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
                if (startTimestamp > latestStartTimestamp[0]) {
                    latestStartTimestamp[0] = startTimestamp;
                    latestRecordingId[0] = recordingId;
                }
            });
        if (latestRecordingId[0] < 0) {
            throw new IllegalStateException("저널 스트림의 녹화를 카탈로그에서 찾지 못했습니다");
        }
        return latestRecordingId[0];
    }

    /**
     * 저널러(2b-1 게이팅 핸들러) 단일 스레드만 쓰는 발행 스트림이라 {@link ExclusivePublication}으로
     * 연다({@code Publication}보다 락 없이 더 가볍다 — 단일 발행자 전제가 성립할 때만 안전하다).
     */
    @Bean(destroyMethod = "close")
    public ExclusivePublication accountJournalPublication(Aeron aeron, Long accountJournalRecordingSubscriptionId) {
        return aeron.addExclusivePublication(JOURNAL_CHANNEL, JOURNAL_STREAM_ID);
    }

    @Bean
    public AeronArchiveAccountJournal accountJournal(ExclusivePublication accountJournalPublication) {
        return new AeronArchiveAccountJournal(accountJournalPublication);
    }
}
