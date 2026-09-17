package com.flab.stocktradingengine.account.worker.config;

import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

import com.flab.stocktradingengine.account.worker.lifecycle.AccountFillReceiverLifecycle;
import com.flab.stocktradingengine.account.worker.lifecycle.OrderResultForwarderLifecycle;
import com.flab.stocktradingengine.account.worker.messaging.OrderResultForwarder;
import com.flab.stocktradingengine.aeron.AeronStreamIds;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;

/**
 * 주문 결과 기록 스트림을 Aeron Archive로 durable 녹화하는 배선. {@link AccountJournalArchiveConfig}를
 * 본뜨되, 결과를 읽는 쪽(U3-a {@link OrderResultForwarder})이 라이브 Subscription이 아니라 Archive
 * replay를 쓴다 — 느린 구독자가 U2의 결과 Publication을 막아 핫패스로 지연이 전파되는 것을 재현으로
 * 확인했기 때문이다(워크트리 루트 {@code _lld_order_result_record.md}·
 * {@code _investigation_order_result_backpressure.md} 참고).
 *
 * <h3>순서 (녹화·조회 유실 방지)</h3>
 * <p>①{@link #orderResultRecordingSubscriptionId}(녹화 시작) → ②{@link #orderResultPublication}
 * (U2가 쓰는 발행 스트림)·{@link #orderResultRecordingId}(카탈로그 조회) → ③
 * {@link #orderResultReplaySubscription}(U3-a가 읽는 replay) 순서로 만들어지도록 각자 앞 단계
 * 빈을 파라미터로 받아 의존시킨다 — {@link AccountJournalArchiveConfig}와 같은 이유(녹화가 발행보다
 * 늦게 시작되면 그 사이 엔트리가 빠지고, 카탈로그 조회는 녹화가 실제로 시작된 뒤에야 recordingId가
 * 나타난다).</p>
 */
@Configuration
public class OrderResultArchiveConfig {

    public static final String ORDER_RESULT_CHANNEL = "aeron:ipc";
    // 결과 발행 스트림(core AeronStreamIds.ORDER_RESULT=4007)과 구분되는, replay 전용 스트림 —
    // AccountJournalReplayer.REPLAY_STREAM_ID(4006)와 같은 이유로 로컬에 둔다.
    private static final int ORDER_RESULT_REPLAY_STREAM_ID = 4008;
    // listRecordingsForUri 페이지 크기 — AccountJournalArchiveConfig와 같은 값.
    private static final int LIST_RECORDINGS_LIMIT = 100;

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

    /**
     * 이 프로세스가 이번 실행에서 시작한 결과 스트림 recording의 ID — {@code
     * AccountJournalArchiveConfig#accountJournalRecordingId}와 같은 방식(카탈로그에서 시작 시각이
     * 가장 최근인 것). {@link #orderResultRecordingSubscriptionId}에 의존해 그 녹화가 실제로 시작된
     * 뒤에만 조회한다.
     *
     * <h3>한계 — U3-b에서 recordingId·position을 이어받기 전까지</h3>
     * <p>재시작하면 이전 실행의 recording은 여기서 안 보이고 이번 실행의 새 recording만 잡힌다.
     * 그래서 U3-a의 {@link OrderResultForwarder}는 이전 실행 구간에 남아 있던 미전송 판정을 못
     * 본다(유실, U3-b에서 닫는다 — LLD "왜 라이브 Subscription이 아니라 Archive replay인가" 절
     * 참고).</p>
     */
    @Bean
    public Long orderResultRecordingId(AeronArchive aeronArchive, Long orderResultRecordingSubscriptionId) {
        long[] latestRecordingId = {-1L};
        long[] latestStartTimestamp = {Long.MIN_VALUE};
        aeronArchive.listRecordingsForUri(0, LIST_RECORDINGS_LIMIT, ORDER_RESULT_CHANNEL, AeronStreamIds.ORDER_RESULT,
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
             startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
             mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
                if (startTimestamp > latestStartTimestamp[0]) {
                    latestStartTimestamp[0] = startTimestamp;
                    latestRecordingId[0] = recordingId;
                }
            });
        if (latestRecordingId[0] < 0) {
            throw new IllegalStateException("주문 결과 스트림의 녹화를 카탈로그에서 찾지 못했습니다");
        }
        return latestRecordingId[0];
    }

    /**
     * {@link OrderResultForwarder}가 읽는 replay. {@code length=Long.MAX_VALUE}로 열어 recording이
     * 아직 stop되지 않은(진행 중인) 상태에서도 라이브로 계속 따라간다(공식 javadoc·재현으로 확인 —
     * {@code _investigation_order_result_backpressure.md}). position=0부터 읽는다(U3-a는 이번
     * 실행 recording 전체를 처음부터 읽는다 — U3-b에서 이어받는 position으로 바뀐다).
     */
    @Bean(destroyMethod = "close")
    public Subscription orderResultReplaySubscription(AeronArchive aeronArchive, Long orderResultRecordingId) {
        return aeronArchive.replay(
            orderResultRecordingId, 0L, Long.MAX_VALUE, ORDER_RESULT_CHANNEL, ORDER_RESULT_REPLAY_STREAM_ID);
    }

    @Bean
    public OrderResultForwarder orderResultForwarder(
            Subscription orderResultReplaySubscription, KafkaTemplate<String, Object> kafkaTemplate) {
        return new OrderResultForwarder(orderResultReplaySubscription, kafkaTemplate);
    }

    /** {@link AccountFillReceiverLifecycle}과 같은 phase(엔진 기본 phase 0보다 늦게 시작). */
    @Bean
    public SmartLifecycle orderResultForwarderLifecycle(OrderResultForwarder orderResultForwarder) {
        return new OrderResultForwarderLifecycle(orderResultForwarder);
    }
}
