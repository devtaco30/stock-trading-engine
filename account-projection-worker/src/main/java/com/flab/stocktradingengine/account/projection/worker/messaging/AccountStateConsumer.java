package com.flab.stocktradingengine.account.projection.worker.messaging;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.flab.stocktradingengine.account.projection.worker.service.AccountProjectionUpserter;
import com.flab.stocktradingengine.kafka.event.AccountStateEvent;
import com.flab.stocktradingengine.time.StageLatencyRegistry;

/**
 * {@code account-state} 토픽을 배치로 소비해 {@link AccountProjectionUpserter}로 read model
 * 반영을 위임한다(계좌 상태 영속/프로젝션 트랙 U3-ii — 배치 안 같은 계좌 이벤트는 upserter가
 * 최신 seq 하나로 합친다). {@code spring.kafka.listener.type: batch}(application.yml)로 이
 * 리스너가 한 poll 배치를 통째로 받는다. ack는 배치 반영(=커밋)이 끝난 뒤 배치 전체에 한 번
 * 보낸다(커밋 전에 ack가 나가면 커밋 실패 시 오프셋만 넘어가 상태 갱신이 유실된다).
 *
 * <h3>구간 분해 측정(measure/segments 브랜치 한정)</h3>
 * <p>{@code measure.stages.enabled}가 켜져 있으면 커밋이 끝난 직후에 두 단계를 기록한다.
 * {@code db_reflect}는 이벤트마다 (커밋 직후 시각 − 이벤트의 {@code epochMillis})이고, 이벤트 시각은 account-worker가
 * 계좌 상태를 바꾼 직후에 찍은 값이다. 배치 안에서 같은 계좌의 앞선 이벤트는 upserter가 건너뛰지만, 그 계좌의 최신
 * 상태가 DB에 반영된 시각을 기준으로 모든 이벤트에 같은 방식으로 기록한다. 두 시각 모두 밀리초 해상도라 값에
 * ±1ms 오차가 있다. {@code db_batch}는 배치 한 번의 반영 시간이다. 꺼져 있으면 시각도 읽지 않는다.</p>
 */
@Component
public class AccountStateConsumer {

    private static final Logger log = Logger.getLogger(AccountStateConsumer.class.getName());
    private static final long NANOS_PER_MILLI = 1_000_000L;

    private final AccountProjectionUpserter accountProjectionUpserter;
    private final StageLatencyRegistry stageLatencyRegistry;

    public AccountStateConsumer(AccountProjectionUpserter accountProjectionUpserter,
            StageLatencyRegistry stageLatencyRegistry) {
        this.accountProjectionUpserter = accountProjectionUpserter;
        this.stageLatencyRegistry = stageLatencyRegistry;
    }

    @KafkaListener(topics = "account-state", groupId = "account-projection-worker")
    public void consume(List<AccountStateEvent> events, Acknowledgment ack) {
        boolean measure = stageLatencyRegistry.isEnabled();
        long batchStartNanos = measure ? System.nanoTime() : 0L;

        accountProjectionUpserter.upsertBatch(events);

        if (measure) {
            recordStages(events, batchStartNanos);
        }
        ack.acknowledge();
    }

    private void recordStages(List<AccountStateEvent> events, long batchStartNanos) {
        try {
            long committedNanos = System.nanoTime();
            long committedMillis = System.currentTimeMillis();
            stageLatencyRegistry.stage("db_batch").recordNanos(committedNanos - batchStartNanos);
            for (AccountStateEvent event : events) {
                long reflectMillis = Math.max(0L, committedMillis - event.epochMillis());
                stageLatencyRegistry.stage("db_reflect").recordNanos(reflectMillis * NANOS_PER_MILLI);
            }
        } catch (RuntimeException e) {
            log.log(Level.WARNING, "[구간 지연 측정] projection 단계 기록 실패(계측만 스킵, 처리는 계속됨)", e);
        }
    }
}
