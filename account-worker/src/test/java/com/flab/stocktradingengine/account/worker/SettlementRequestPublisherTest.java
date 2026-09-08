package com.flab.stocktradingengine.account.worker;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import com.flab.stocktradingengine.kafka.KafkaTopics;
import com.flab.stocktradingengine.kafka.event.SettlementRequestEvent;

/**
 * 매수 체결이 남긴 미수금(onUnpaidRecorded)이 settlement-requests 토픽으로 정확히 발행되는지
 * 검증한다. dueAtEpochMillis는 T+2(영업일 미고려)이므로 시각을 고정한 Clock으로 계산한다.
 */
class SettlementRequestPublisherTest {

    private static final String TOPIC = KafkaTopics.settlementRequests();

    @Test
    void 미수금이_생기면_accountId_키로_settlementRef_dueAtEpochMillis를_실어_발행한다() {
        KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
        Clock fixedClock = Clock.fixed(Instant.parse("2026-09-08T00:00:00Z"), ZoneOffset.UTC);
        SettlementRequestPublisher publisher = new SettlementRequestPublisher(kafkaTemplate, fixedClock);

        publisher.onUnpaidRecorded(1L, 9001L, new BigDecimal("60000"));

        long expectedDueAt = fixedClock.instant().plus(Duration.ofDays(2)).toEpochMilli();
        SettlementRequestEvent expected =
            new SettlementRequestEvent(9001L, 1L, new BigDecimal("60000"), expectedDueAt);
        verify(kafkaTemplate).send(eq(TOPIC), eq("1"), eq(expected));
    }

    @Test
    void 미수금이_없는_다른_콜백은_아무것도_발행하지_않는다() {
        KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
        SettlementRequestPublisher publisher = new SettlementRequestPublisher(kafkaTemplate, Clock.systemUTC());

        publisher.onFillApplied(1L, 10L, 9001L, true);
        publisher.onAccepted(1L, 10L, "r1", new BigDecimal("40000"));

        verifyNoInteractions(kafkaTemplate);
    }
}
