package com.flab.stocktradingengine.matching.worker;

import java.util.List;
import java.util.Optional;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.matching.disruptor.AeronArchiveMatchingJournal;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;

/**
 * 매칭 저널을 Aeron Archive로 durable 녹화하는 배선(2c-1) + 이전 녹화를 리플레이해 복구 입력을
 * 만드는 배선(2c-2). {@link MatchingOrderIntakeConfig}가 만든 matching-worker 자체
 * Aeron·Archive(ArchivingMediaDriver)를 그대로 쓴다. account-worker
 * {@code AccountJournalArchiveConfig}와 같은 결이다.
 *
 * <h3>순서 (저널 유실·오염 방지)</h3>
 * <p>①{@link #matchingJournalRecoveredEntries}(이전 녹화 읽기, 2c-2) → ②
 * {@link #matchingJournalRecordingSubscriptionId}(새 녹화 시작) → ③
 * {@link #matchingJournalPublication}(발행 스트림) 순서로 만들어지도록 각자 앞 단계 빈을
 * 파라미터로 받아 의존시킨다. ①이 ②보다 먼저여야 하는 이유는, ②가 시작되기 전엔 이번 실행의
 * 새 녹화가 카탈로그에 아직 없어(Archive는 이미지가 실제로 붙어야 recordingId를 만든다) 리플레이가
 * 과거 녹화만 읽는다는 게 보장되기 때문이다 — 순서가 바뀌면 리플레이가 이번 실행 자신의(아직
 * 비어있는) 녹화까지 스캔 대상에 넣을 여지가 생긴다. ②가 ③보다 먼저여야 하는 이유는 IPC에서 새
 * 구독자는 붙은 시점 이후 데이터만 보므로, 녹화가 발행보다 늦게 시작되면 그 사이 엔트리가 빠진다.</p>
 */
@Configuration
public class MatchingJournalArchiveConfig {

    // 패키지 가시성 — 테스트가 프로덕션과 같은 채널·스트림을 조회하도록 이 상수를 그대로 참조한다.
    static final String JOURNAL_CHANNEL = "aeron:ipc";
    static final int JOURNAL_STREAM_ID = 2005; // 매칭 인테이크(2002)와 구분되는 저널 전용 스트림

    /**
     * 이 스트림에서 복구 입력을 읽어 만든다.
     * {@link com.flab.stocktradingengine.matching.disruptor.MatchingEngine#recover}에 그대로 넘긴다
     * ({@code MatchingEngineConfig} 참고). 반드시 {@link #matchingJournalRecordingSubscriptionId}
     * 보다 먼저 만들어져야 한다(클래스 javadoc "순서" 참고) — 이 빈 자체가 그 순서를 강제한다.
     *
     * <p>스냅샷이 있으면(2d-1b) 스냅샷이 가리키는 recordingId·position부터만 읽는다 — 스냅샷이
     * 이미 그 앞까지의 상태를 담고 있어 처음부터 다시 읽을 이유가 없다. 스냅샷이 없으면(2c-2,
     * 하위호환) 이전 녹화 전부를 처음부터 읽는다.</p>
     */
    @Bean
    public List<JournaledOrder> matchingJournalRecoveredEntries(
            AeronArchive aeronArchive, Optional<StoredMatchingSnapshot> matchingLoadedSnapshot) {
        MatchingJournalReplayer replayer = new MatchingJournalReplayer(aeronArchive);
        if (matchingLoadedSnapshot.isPresent()) {
            StoredMatchingSnapshot stored = matchingLoadedSnapshot.get();
            return replayer.readFrom(JOURNAL_CHANNEL, JOURNAL_STREAM_ID, stored.recordingId(), stored.snapshot().journalPosition());
        }
        return replayer.readAll(JOURNAL_CHANNEL, JOURNAL_STREAM_ID);
    }

    /**
     * 저널 채널·스트림에 새 녹화를 시작한다. 반환값(Archive 구독 ID)은 안 쓴다 — 이 빈이 존재하는
     * 이유는 {@link #matchingJournalPublication}이 이 빈에 의존하게 만들어 생성 순서를 강제하는
     * 것뿐이다(클래스 javadoc "순서" 참고). {@code matchingJournalRecoveredEntries}를 파라미터로
     * 받는 이유도 같다 — 리플레이가 끝난 뒤에야 새 녹화를 시작해야 한다.
     */
    @Bean
    public Long matchingJournalRecordingSubscriptionId(AeronArchive aeronArchive, List<JournaledOrder> matchingJournalRecoveredEntries) {
        return aeronArchive.startRecording(JOURNAL_CHANNEL, JOURNAL_STREAM_ID, SourceLocation.LOCAL);
    }

    /**
     * 저널러(2c-1 게이팅 핸들러) 단일 스레드만 쓰는 발행 스트림이라 {@link ExclusivePublication}으로
     * 연다({@code Publication}보다 락 없이 더 가볍다 — 단일 발행자 전제가 성립할 때만 안전하다).
     */
    @Bean(destroyMethod = "close")
    public ExclusivePublication matchingJournalPublication(Aeron aeron, Long matchingJournalRecordingSubscriptionId) {
        return aeron.addExclusivePublication(JOURNAL_CHANNEL, JOURNAL_STREAM_ID);
    }

    @Bean
    public AeronArchiveMatchingJournal matchingJournal(ExclusivePublication matchingJournalPublication) {
        return new AeronArchiveMatchingJournal(matchingJournalPublication);
    }
}
