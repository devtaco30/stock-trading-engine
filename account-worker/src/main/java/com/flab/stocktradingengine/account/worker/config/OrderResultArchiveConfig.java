package com.flab.stocktradingengine.account.worker.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.aeron.AeronStreamIds;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;

/**
 * 주문 결과 기록 스트림을 Aeron Archive로 durable 녹화하는 배선. {@link AccountJournalArchiveConfig}를
 * 본뜨되, 이 트랙엔 리플레이 복구(U3-b)가 아직 없어 발행 스트림만 연다 — 녹화(①)가 발행(②)보다
 * 먼저 시작돼야 하는 이유는 저널과 같다(IPC 새 구독자는 붙은 시점 이후 데이터만 본다).
 */
@Configuration
public class OrderResultArchiveConfig {

    public static final String ORDER_RESULT_CHANNEL = "aeron:ipc";

    @Bean
    public Long orderResultRecordingSubscriptionId(AeronArchive aeronArchive) {
        return aeronArchive.startRecording(ORDER_RESULT_CHANNEL, AeronStreamIds.ORDER_RESULT, SourceLocation.LOCAL);
    }

    /**
     * {@code OrderResultRecorder}(계좌 엔진 단일 스레드) 하나만 쓰는 발행 스트림이라
     * {@link ExclusivePublication}으로 연다.
     */
    @Bean(destroyMethod = "close")
    public ExclusivePublication orderResultPublication(Aeron aeron, Long orderResultRecordingSubscriptionId) {
        return aeron.addExclusivePublication(ORDER_RESULT_CHANNEL, AeronStreamIds.ORDER_RESULT);
    }
}
