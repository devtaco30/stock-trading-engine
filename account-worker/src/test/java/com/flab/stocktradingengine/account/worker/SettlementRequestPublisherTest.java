package com.flab.stocktradingengine.account.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import com.flab.stocktradingengine.kafka.KafkaTopics;
import com.flab.stocktradingengine.kafka.event.SettlementRequestEvent;

/**
 * 매수 체결이 남긴 미수금(onUnpaidRecorded)이 settlement-requests 토픽으로 정확히 발행되는지
 * 검증한다. dueAtEpochMillis는 T+2(영업일 미고려)다.
 */
class SettlementRequestPublisherTest {

    private static final String TOPIC = KafkaTopics.settlementRequests();

    @Test
    void T2_만기_계산은_now에_이틀을_더한다() {
        long fixedNow = Instant.parse("2026-09-08T00:00:00Z").toEpochMilli();

        long dueAt = SettlementRequestPublisher.dueAtEpochMillis(fixedNow);

        assertEquals(fixedNow + Duration.ofDays(2).toMillis(), dueAt);
    }

    @Test
    void 미수금이_생기면_accountId_키로_settlementRef_dueAtEpochMillis를_실어_발행한다() {
        KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
        SettlementRequestPublisher publisher = new SettlementRequestPublisher(kafkaTemplate);

        long before = Instant.now().toEpochMilli();
        publisher.onUnpaidRecorded(1L, 9001L, new BigDecimal("60000"));
        long after = Instant.now().toEpochMilli();

        ArgumentCaptor<SettlementRequestEvent> captor = ArgumentCaptor.forClass(SettlementRequestEvent.class);
        verify(kafkaTemplate).send(eq(TOPIC), eq("1"), captor.capture());
        SettlementRequestEvent event = captor.getValue();

        assertEquals(9001L, event.settlementRef());
        assertEquals(1L, event.accountId());
        assertEquals(0, new BigDecimal("60000").compareTo(event.amount()));
        // Instant.now() 기반이라 정확한 값은 못 박지 못한다 — 호출 전후로 잰 범위 안인지만 확인한다.
        assertThat(event.dueAtEpochMillis()).isBetween(
            before + Duration.ofDays(2).toMillis(), after + Duration.ofDays(2).toMillis());
    }

    @Test
    void 미수금이_없는_다른_콜백은_아무것도_발행하지_않는다() {
        KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
        SettlementRequestPublisher publisher = new SettlementRequestPublisher(kafkaTemplate);

        publisher.onFillApplied(1L, 10L, 9001L, true);
        publisher.onAccepted(1L, 10L, "r1", new BigDecimal("40000"));

        verifyNoInteractions(kafkaTemplate);
    }
}
