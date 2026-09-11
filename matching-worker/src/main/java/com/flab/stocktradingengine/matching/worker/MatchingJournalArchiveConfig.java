package com.flab.stocktradingengine.matching.worker;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.matching.disruptor.AeronArchiveMatchingJournal;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;

/**
 * 매칭 저널을 Aeron Archive로 durable 녹화하는 배선(2c-1). {@link MatchingOrderIntakeConfig}가
 * 만든 matching-worker 자체 Aeron·Archive(ArchivingMediaDriver)를 그대로 쓴다.
 * account-worker {@code AccountJournalArchiveConfig}와 같은 결이다.
 *
 * <h3>녹화 시작 순서 (저널 유실 방지)</h3>
 * <p>{@link #matchingJournalRecordingSubscriptionId}가 {@link #matchingJournalPublication}보다
 * 먼저 만들어지도록 그 빈을 파라미터로 받아 의존시킨다 — Archive가 이 채널·스트림 녹화를 먼저
 * 시작한 뒤에야 우리 발행 스트림(ExclusivePublication)이 열린다. IPC에서 새 구독자는 자신이
 * 붙은 시점 이후의 데이터만 보므로, 녹화(=Archive 내부 구독)가 발행보다 늦게 시작되면 그 사이
 * 기록된 저널 엔트리가 녹화에서 빠진다.</p>
 */
@Configuration
public class MatchingJournalArchiveConfig {

    // 패키지 가시성 — 테스트가 프로덕션과 같은 채널·스트림을 조회하도록 이 상수를 그대로 참조한다.
    static final String JOURNAL_CHANNEL = "aeron:ipc";
    static final int JOURNAL_STREAM_ID = 2005; // 매칭 인테이크(2002)와 구분되는 저널 전용 스트림

    /**
     * 저널 채널·스트림 녹화를 시작한다. 반환값(Archive 구독 ID)은 안 쓴다 — 이 빈이 존재하는 이유는
     * {@link #matchingJournalPublication}이 이 빈에 의존하게 만들어 생성 순서를 강제하는 것뿐이다
     * (클래스 javadoc "녹화 시작 순서" 참고).
     */
    @Bean
    public Long matchingJournalRecordingSubscriptionId(AeronArchive aeronArchive) {
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
