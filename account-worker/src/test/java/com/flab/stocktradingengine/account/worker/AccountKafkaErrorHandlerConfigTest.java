package com.flab.stocktradingengine.account.worker;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListenerContainer;

import com.flab.stocktradingengine.account.disruptor.JournalUnavailableException;

/**
 * A-3 에러 핸들러 라우팅 검증 — 저널 죽음일 때만 컨테이너를 멈추고, 그 외 예외는 안 멈춘다.
 *
 * <p>실제 리스너는 예외를 {@link ListenerExecutionFailedException}로 감싸므로, cause 체인을
 * 따라가 원인 타입으로 위임 대상을 고르는지도 함께 확인한다(감싼 채로 넣는다).</p>
 */
class AccountKafkaErrorHandlerConfigTest {

    private final CommonErrorHandler handler = new AccountKafkaErrorHandlerConfig().kafkaErrorHandler();

    private static List<ConsumerRecord<?, ?>> oneRecord() {
        // 실제 topic/partition/offset을 준다 — DefaultErrorHandler가 seek 계산 때 null로 터지지 않게.
        return List.of(new ConsumerRecord<>("account-fills", 0, 0L, "k", "v"));
    }

    @Test
    @DisplayName("저널 죽음(JournalUnavailableException)이면 컨테이너를 멈춘다")
    void 저널죽음이면_컨테이너정지() throws InterruptedException {
        MessageListenerContainer container = mock(MessageListenerContainer.class);
        when(container.isRunning()).thenReturn(true); // 멈출 대상이 돌고 있어야 stopping 핸들러가 stop을 부른다
        CountDownLatch stopped = new CountDownLatch(1);
        // setStopContainerAbnormally(true) 라서 stopping 핸들러는 stopAbnormally(Runnable)를 부른다.
        doAnswer(inv -> {
            stopped.countDown();
            return null;
        }).when(container).stopAbnormally(any(Runnable.class));

        Consumer<?, ?> consumer = mock(Consumer.class);
        Exception wrapped = new ListenerExecutionFailedException("리스너 실패",
            new JournalUnavailableException("저널이 못 씀"));
        try {
            // CommonContainerStoppingErrorHandler는 offset 커밋을 막으려 예외를 다시 던진다 — 정상.
            handler.handleRemaining(wrapped, oneRecord(), consumer, container);
        } catch (Exception rethrown) {
            // 무시: 컨테이너 정지가 실제 검증 대상이다.
        }

        assertTrue(stopped.await(2, TimeUnit.SECONDS), "저널 죽음이면 컨테이너가 멈춰야 한다");
    }

    @Test
    @DisplayName("그 외 예외는 컨테이너를 멈추지 않는다(기본 처리로 위임)")
    void 그외예외는_컨테이너를_안멈춘다() {
        MessageListenerContainer container = mock(MessageListenerContainer.class);
        Consumer<?, ?> consumer = mock(Consumer.class);
        Exception wrapped = new ListenerExecutionFailedException("리스너 실패",
            new RuntimeException("메시지 자체 문제(poison 등)"));
        try {
            handler.handleRemaining(wrapped, oneRecord(), consumer, container);
        } catch (Exception ignored) {
            // 기본 핸들러가 던지더라도, 검증은 "컨테이너를 안 멈춘다"이다.
        }

        verify(container, never()).stop();
        verify(container, never()).stop(any(Runnable.class));
        verify(container, never()).stopAbnormally(any(Runnable.class));
    }
}
