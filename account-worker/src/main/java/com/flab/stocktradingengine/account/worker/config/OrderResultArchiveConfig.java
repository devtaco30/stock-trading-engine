package com.flab.stocktradingengine.account.worker.config;

import java.io.File;
import java.util.List;
import java.util.Optional;

import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

import com.flab.stocktradingengine.account.worker.lifecycle.AccountFillReceiverLifecycle;
import com.flab.stocktradingengine.account.worker.lifecycle.OrderResultForwarderLifecycle;
import com.flab.stocktradingengine.account.worker.messaging.OrderResultForwarder;
import com.flab.stocktradingengine.account.worker.recovery.OrderResultCatchUpReplayer;
import com.flab.stocktradingengine.account.worker.recovery.OrderResultForwardPositionStore;
import com.flab.stocktradingengine.account.worker.recovery.StoredOrderResultForwardPosition;
import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.codec.OrderResultEntry;

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
 * <h3>순서 (캐치업·녹화·조회 유실 방지)</h3>
 * <p>①{@link #orderResultCatchUpEntries}(과거 recording 읽기, U3-b) → ②
 * {@link #orderResultRecordingSubscriptionId}(이번 실행 녹화 시작) → ③{@link #orderResultPublication}
 * (U2가 쓰는 발행 스트림)·{@link #orderResultRecordingId}(카탈로그 조회) → ④
 * {@link #orderResultCatchUpForwarded}(캐치업분을 Kafka로 보내고 position 마킹) → ⑤
 * {@link #orderResultReplaySubscription}(U3-a가 읽는 라이브 replay) 순서로 만들어지도록 각자
 * 앞 단계 빈을 파라미터로 받아 의존시킨다. ①이 ②보다 먼저인 이유는 {@link AccountJournalArchiveConfig}와
 * 같다 — ②가 시작되기 전엔 이번 실행의 새 recording이 카탈로그에 아직 없어 캐치업이 과거
 * recording만 읽는다는 게 보장된다.</p>
 */
@Configuration
public class OrderResultArchiveConfig {

    public static final String ORDER_RESULT_CHANNEL = "aeron:ipc";
    // 결과 발행 스트림(core AeronStreamIds.ORDER_RESULT=4007)과 구분되는, 라이브 replay 전용
    // 스트림 — AccountJournalReplayer.REPLAY_STREAM_ID(4006)와 같은 이유로 로컬에 둔다.
    // 캐치업 replay(과거 recording)는 별도 스트림(4009, OrderResultCatchUpReplayer 내부)을 쓴다.
    private static final int ORDER_RESULT_REPLAY_STREAM_ID = 4008;
    // listRecordingsForUri 페이지 크기 — AccountJournalArchiveConfig와 같은 값.
    private static final int LIST_RECORDINGS_LIMIT = 100;

    /**
     * 이전 실행에서 아직 Kafka로 못 보낸 판정을 과거(이미 멈춘) recording에서 읽어 온다(U3-b).
     * {@link OrderResultForwardPositionStore}에 저장된 위치가 있으면 거기부터, 없으면(첫 기동)
     * 전부 읽는다 — {@code AccountJournalArchiveConfig#accountJournalRecoveredEntries}와 같은 결.
     */
    @Bean
    public List<OrderResultEntry> orderResultCatchUpEntries(
            AeronArchive aeronArchive, OrderResultForwardPositionStore orderResultForwardPositionStore) {
        OrderResultCatchUpReplayer replayer = new OrderResultCatchUpReplayer(aeronArchive);
        Optional<StoredOrderResultForwardPosition> stored = orderResultForwardPositionStore.read();
        if (stored.isPresent()) {
            StoredOrderResultForwardPosition position = stored.get();
            return replayer.readFrom(ORDER_RESULT_CHANNEL, AeronStreamIds.ORDER_RESULT, position.recordingId(), position.position());
        }
        return replayer.readAll(ORDER_RESULT_CHANNEL, AeronStreamIds.ORDER_RESULT);
    }

    @Bean
    public Long orderResultRecordingSubscriptionId(AeronArchive aeronArchive, List<OrderResultEntry> orderResultCatchUpEntries) {
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

    @Bean
    public OrderResultForwardPositionStore orderResultForwardPositionStore(File accountArchiveDir) {
        return new OrderResultForwardPositionStore(accountArchiveDir);
    }

    /**
     * 캐치업분({@link #orderResultCatchUpEntries})을 Kafka로 보내고, 이번 실행의 recording을
     * position 0부터 보는 새 기준점으로 저장한다(U3-b). {@link #orderResultReplaySubscription}이
     * 이 빈에 의존해, 캐치업이 끝난 뒤에만 라이브 replay가 열리도록 순서를 강제한다.
     */
    @Bean
    public Integer orderResultCatchUpForwarded(
            List<OrderResultEntry> orderResultCatchUpEntries,
            KafkaTemplate<String, Object> kafkaTemplate,
            OrderResultForwardPositionStore orderResultForwardPositionStore,
            Long orderResultRecordingId) {
        for (OrderResultEntry entry : orderResultCatchUpEntries) {
            OrderResultForwarder.sendBlocking(kafkaTemplate, entry); // 성공할 때까지 블록 — 실패한 채 position을 0으로 마킹하지 않기 위함
        }
        orderResultForwardPositionStore.write(orderResultRecordingId, 0L);
        return orderResultCatchUpEntries.size();
    }

    /**
     * {@link OrderResultForwarder}가 읽는 라이브 replay. {@code length=Long.MAX_VALUE}로 열어
     * recording이 아직 stop되지 않은(진행 중인) 상태에서도 라이브로 계속 따라간다(공식 javadoc·
     * 재현으로 확인 — {@code _investigation_order_result_backpressure.md}). position=0부터
     * 읽는다 — 이번 실행의 새 recording은 캐치업 대상이 아니라 항상 처음부터 본다.
     */
    @Bean(destroyMethod = "close")
    public Subscription orderResultReplaySubscription(
            AeronArchive aeronArchive, Long orderResultRecordingId, Integer orderResultCatchUpForwarded) {
        return aeronArchive.replay(
            orderResultRecordingId, 0L, Long.MAX_VALUE, ORDER_RESULT_CHANNEL, ORDER_RESULT_REPLAY_STREAM_ID);
    }

    @Bean
    public OrderResultForwarder orderResultForwarder(
            Subscription orderResultReplaySubscription, KafkaTemplate<String, Object> kafkaTemplate,
            OrderResultForwardPositionStore orderResultForwardPositionStore, Long orderResultRecordingId) {
        return new OrderResultForwarder(
            orderResultReplaySubscription, kafkaTemplate, orderResultForwardPositionStore, orderResultRecordingId);
    }

    /** {@link AccountFillReceiverLifecycle}과 같은 phase(엔진 기본 phase 0보다 늦게 시작). */
    @Bean
    public SmartLifecycle orderResultForwarderLifecycle(OrderResultForwarder orderResultForwarder) {
        return new OrderResultForwarderLifecycle(orderResultForwarder);
    }
}
