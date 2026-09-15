package com.flab.stocktradingengine.account.worker.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.math.BigDecimal;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import com.flab.stocktradingengine.kafka.KafkaTopics;
import com.flab.stocktradingengine.kafka.event.AccountStateEvent;

/**
 * 잔고·보유 변경(onStateChanged)이 account-state 토픽으로 정확히 발행되는지 검증한다(계좌 상태
 * 영속/프로젝션 트랙 Unit 2). 발행이 전용 publisher 스레드를 거치는 비동기 구조라
 * {@link org.mockito.Mockito#timeout(long)}로 드레인을 기다린다({@link SettlementRequestPublisherTest}와
 * 달리 리스너 호출 즉시 send되지 않는다).
 */
class AccountStatePublisherTest {

    private static final String TOPIC = KafkaTopics.accountState();

    private AccountStatePublisher publisher;

    @AfterEach
    void tearDown() {
        if (publisher != null) {
            publisher.close();
        }
    }

    @Test
    void 상태변경_콜백이_오면_전용_스레드를_통해_account_state_토픽으로_발행한다() {
        KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
        publisher = new AccountStatePublisher(kafkaTemplate);
        publisher.start();

        Map<String, Integer> holdings = Map.of("005930", 10);
        long before = System.currentTimeMillis();
        publisher.onStateChanged(1L, new BigDecimal("900000"), holdings, 3L);
        long after = System.currentTimeMillis();

        ArgumentCaptor<AccountStateEvent> captor = ArgumentCaptor.forClass(AccountStateEvent.class);
        verify(kafkaTemplate, timeout(1000)).send(eq(TOPIC), eq("1"), captor.capture());
        AccountStateEvent event = captor.getValue();

        assertEquals(1L, event.accountId());
        assertEquals(0, new BigDecimal("900000").compareTo(event.balance()));
        assertEquals(holdings, event.holdings());
        assertEquals(3L, event.seq());
        assertEquals(true, event.epochMillis() >= before && event.epochMillis() <= after);
    }

    @Test
    void 상태변경_외_콜백은_아무것도_발행하지_않는다() {
        KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
        publisher = new AccountStatePublisher(kafkaTemplate);
        publisher.start();

        publisher.onFillApplied(1L, 10L, 9001L, true);
        publisher.onAccepted(1L, 10L, "r1", new BigDecimal("40000"));
        publisher.onSettlementApplied(1L, 7001L, true);
        publisher.onUnpaidRecorded(1L, 9001L, new BigDecimal("60000"));

        verifyNoInteractions(kafkaTemplate);
    }
}
