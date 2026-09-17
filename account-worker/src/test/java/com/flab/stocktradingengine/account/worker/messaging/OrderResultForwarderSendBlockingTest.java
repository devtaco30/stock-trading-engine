package com.flab.stocktradingengine.account.worker.messaging;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import com.flab.stocktradingengine.codec.OrderResultEntry;
import com.flab.stocktradingengine.codec.OrderVerdict;
import com.flab.stocktradingengine.kafka.KafkaTopics;

/**
 * 마지막 보강 — {@link OrderResultForwarder#sendBlocking}이 send 결과를 실제로 확인하는지,
 * 실패하면 같은 엔트리로 재시도해서 결국 성공하는지 검증한다. AccountStatePublisher·
 * SettlementRequestPublisher와 달리 이 트랙은 fire-and-forget이 아니다 — 판정은 사건 하나라
 * 대신할 것이 없기 때문(클래스 javadoc 참고).
 */
class OrderResultForwarderSendBlockingTest {

    @Test
    @DisplayName("send가 바로 성공하면 한 번만 부르고 반환한다")
    void 성공하면_한번에_반환한다() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
        CompletableFuture<SendResult<String, Object>> succeeded = CompletableFuture.completedFuture(null);
        when(kafkaTemplate.send(any(String.class), any(), any())).thenReturn(succeeded);
        OrderResultEntry entry = new OrderResultEntry(1L, 1L, "r1", OrderVerdict.ACCEPTED, null, 1L);

        assertDoesNotThrow(() -> OrderResultForwarder.sendBlocking(kafkaTemplate, entry));

        verify(kafkaTemplate, times(1)).send(eq(KafkaTopics.orderResults()), eq("1"), any());
    }

    @Test
    @DisplayName("send가 두 번 실패해도 같은 엔트리로 재시도해서 세 번째에 성공하면 반환한다")
    void 실패하면_재시도해서_결국_성공한다() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
        CompletableFuture<SendResult<String, Object>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("브로커에 연결할 수 없음(테스트)"));
        CompletableFuture<SendResult<String, Object>> succeeded = CompletableFuture.completedFuture(null);
        when(kafkaTemplate.send(any(String.class), any(), any()))
            .thenReturn(failed)
            .thenReturn(failed)
            .thenReturn(succeeded);
        OrderResultEntry entry = new OrderResultEntry(2L, 2L, "r2", OrderVerdict.ACCEPTED, null, 2L);

        assertDoesNotThrow(() -> OrderResultForwarder.sendBlocking(kafkaTemplate, entry));

        verify(kafkaTemplate, times(3)).send(eq(KafkaTopics.orderResults()), eq("2"), any());
    }
}
