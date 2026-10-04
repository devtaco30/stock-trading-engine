package com.flab.stocktradingengine.account.projection.worker.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.kafka.support.Acknowledgment;

import com.flab.stocktradingengine.account.projection.worker.service.AccountProjectionUpserter;
import com.flab.stocktradingengine.kafka.event.AccountStateEvent;
import com.flab.stocktradingengine.time.StageLatencyRegistry;
import com.flab.stocktradingengine.time.StageSnapshot;

/**
 * 구간 분해 측정의 층 2(계좌 상태 변경부터 DB 커밋까지) 계측 테스트. 이벤트마다 (커밋 직후 시각 − 이벤트 시각)을
 * {@code db_reflect}에, 배치 반영에 걸린 시간을 {@code db_batch}에 기록하고, 기록은 커밋이 끝난 뒤에만 한다.
 */
class AccountStateConsumerStageTest {

    private static AccountStateEvent event(long accountId, long seq, long epochMillis) {
        return new AccountStateEvent(accountId, BigDecimal.TEN, Map.of(), seq, epochMillis);
    }

    @Test
    @DisplayName("계측이 켜져 있으면 이벤트마다 db_reflect를, 배치마다 db_batch를 한 번 기록한다")
    void recordsOneReflectPerEventAndOneBatch() {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        AccountProjectionUpserter upserter = mock(AccountProjectionUpserter.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        AccountStateConsumer consumer = new AccountStateConsumer(upserter, registry);
        long now = System.currentTimeMillis();
        List<AccountStateEvent> events = List.of(event(1, 1, now - 30), event(2, 1, now - 10), event(1, 2, now - 5));

        consumer.consume(events, ack);

        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();
        assertThat(snapshots.get("db_reflect").count()).isEqualTo(3);
        assertThat(snapshots.get("db_batch").count()).isEqualTo(1);
    }

    @Test
    @DisplayName("기록은 upsertBatch가 끝난 뒤이고 ack는 기록 뒤에 보낸다")
    void recordsAfterCommitAndBeforeAck() {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        AccountProjectionUpserter upserter = mock(AccountProjectionUpserter.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        doAnswer(invocation -> {
            Thread.sleep(40);
            return null;
        }).when(upserter).upsertBatch(org.mockito.ArgumentMatchers.anyList());
        AccountStateConsumer consumer = new AccountStateConsumer(upserter, registry);
        List<AccountStateEvent> events = List.of(event(1, 1, System.currentTimeMillis()));

        consumer.consume(events, ack);

        InOrder order = inOrder(upserter, ack);
        order.verify(upserter).upsertBatch(events);
        order.verify(ack).acknowledge();
        StageSnapshot reflect = registry.snapshotAndResetAll().get("db_reflect");
        assertThat(reflect.p50Micros()).as("커밋(40ms 지연)이 끝난 뒤에 시각을 잰 값이어야 한다").isGreaterThanOrEqualTo(30_000);
    }

    @Test
    @DisplayName("계측이 꺼져 있으면 아무것도 기록하지 않고 반영과 ack는 그대로 한다")
    void disabledRecordsNothing() {
        StageLatencyRegistry registry = new StageLatencyRegistry(false);
        AccountProjectionUpserter upserter = mock(AccountProjectionUpserter.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        AccountStateConsumer consumer = new AccountStateConsumer(upserter, registry);
        List<AccountStateEvent> events = List.of(event(1, 1, System.currentTimeMillis()));

        consumer.consume(events, ack);

        verify(upserter).upsertBatch(events);
        verify(ack).acknowledge();
        assertThat(registry.snapshotAndResetAll()).isEmpty();
    }

    @Test
    @DisplayName("이벤트 시각이 커밋 시각보다 늦어도(시계 해상도) 음수로 기록하지 않는다")
    void futureEventTimeIsClampedToZero() {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        AccountProjectionUpserter upserter = mock(AccountProjectionUpserter.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        AccountStateConsumer consumer = new AccountStateConsumer(upserter, registry);
        List<AccountStateEvent> events = List.of(event(1, 1, System.currentTimeMillis() + 5_000));

        consumer.consume(events, ack);

        StageSnapshot reflect = registry.snapshotAndResetAll().get("db_reflect");
        assertThat(reflect.count()).isEqualTo(1);
        assertThat(reflect.maxMicros()).isLessThan(1_000);
    }
}
