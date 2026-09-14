package com.flab.stocktradingengine.matching.worker.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.matching.worker.messaging.AccountFillPublisher;
import com.flab.stocktradingengine.support.SnowflakeIdGenerator;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;

/**
 * 매칭 체결을 Aeron Archive로 durable 녹화하는 배선(ADR-032, U2). {@link MatchingOrderIntakeConfig}가
 * 만든 matching-worker 자체 Aeron·Archive(ArchivingMediaDriver)를 그대로 쓴다.
 * {@link MatchingJournalArchiveConfig}와 같은 결이지만, 이 스트림은 매칭 자기 복구용이 아니라
 * 계좌 축 복구용이라(유닛 4) replay/recover 빈이 없다 — 녹화만 한다.
 *
 * <h3>순서 (전달 유실 방지)</h3>
 * <p>{@link #matchingFillRecordingSubscriptionId}(녹화 시작)가 {@link #matchingFillPublication}
 * (발행 스트림)보다 먼저 만들어지도록 후자가 전자를 파라미터로 받아 의존시킨다 —
 * {@link MatchingJournalArchiveConfig} 클래스 javadoc "순서" 절과 같은 이유다(새 구독자는 붙은
 * 시점 이후 데이터만 보므로, 녹화가 발행보다 늦게 시작되면 그 사이 체결이 빠진다).</p>
 */
@Configuration
public class MatchingFillPublishConfig {

    // 패키지 가시성 — 테스트(같은 패키지)가 프로덕션과 같은 채널을 그대로 참조한다. 실제 채널은
    // transport.fill.channel 속성에서 해석된다(fork1 Unit 1). 스트림 ID는 core
    // AeronStreamIds.FILL로 account-worker와 공유한다(인테이크(2002)·저널(2005)과 구분).
    static final String DEFAULT_FILL_CHANNEL = "aeron:ipc";

    /**
     * 체결 스트림에 새 녹화를 시작한다. 반환값(Archive 구독 ID)은 안 쓴다 — 이 빈이 존재하는
     * 이유는 {@link #matchingFillPublication}이 이 빈에 의존하게 만들어 생성 순서를 강제하는
     * 것뿐이다(클래스 javadoc "순서" 참고).
     */
    @Bean
    public Long matchingFillRecordingSubscriptionId(
            AeronArchive aeronArchive,
            @Value("${transport.fill.channel:" + DEFAULT_FILL_CHANNEL + "}") String fillChannel) {
        return aeronArchive.startRecording(fillChannel, AeronStreamIds.FILL, SourceLocation.LOCAL);
    }

    /**
     * {@link AccountFillPublisher}(매칭 단일 소비자 스레드) 하나만 쓰는 발행 스트림이라
     * {@link ExclusivePublication}으로 연다.
     */
    @Bean(destroyMethod = "close")
    public ExclusivePublication matchingFillPublication(
            Aeron aeron,
            Long matchingFillRecordingSubscriptionId,
            @Value("${transport.fill.channel:" + DEFAULT_FILL_CHANNEL + "}") String fillChannel) {
        return aeron.addExclusivePublication(fillChannel, AeronStreamIds.FILL);
    }

    @Bean
    public AccountFillPublisher accountFillPublisher(
            ExclusivePublication matchingFillPublication, SnowflakeIdGenerator snowflakeIdGenerator) {
        return new AccountFillPublisher(matchingFillPublication, snowflakeIdGenerator);
    }
}
