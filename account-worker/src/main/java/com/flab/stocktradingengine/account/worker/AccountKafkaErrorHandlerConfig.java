package com.flab.stocktradingengine.account.worker;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;
import org.springframework.kafka.listener.CommonDelegatingErrorHandler;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;

import com.flab.stocktradingengine.account.disruptor.JournalUnavailableException;

/**
 * account-worker Kafka 리스너의 에러 처리 정책(A-3). Spring Boot가 단일 {@link CommonErrorHandler}
 * 빈을 기본 리스너 컨테이너 팩토리에 자동 적용한다(검증: Boot 3.4.0
 * {@code ConcurrentKafkaListenerContainerFactoryConfigurer.setCommonErrorHandler}).
 *
 * <h3>두 실패를 갈라서 다룬다</h3>
 * <ul>
 *   <li><b>저널 죽음</b>({@link JournalUnavailableException}) — durable 저장소(Aeron Archive)가
 *       지금 못 쓴다는 인프라 신호다. 메시지 자체는 멀쩡하므로 드롭(DLQ)하면 안 된다. 컨테이너를
 *       멈춰(backpressure) offset이 넘어가지 못하게 해 유실을 막고, 재시작 시 저널 replay로
 *       복구한다(재시작은 K8s 등 배포 계층). Kafka Streams가 changelog 기록 실패를 fatal로 다루고
 *       재기동 시 changelog에서 복원하는 것과 같은 모델(KIP-572, task.timeout.ms 계열).</li>
 *   <li><b>그 외</b>(역직렬화 실패 등 메시지 자체 문제) — Spring 기본 {@link DefaultErrorHandler}
 *       (재시도 후 스킵). 이건 poison이라 한 건만 걸러내고 계속 간다. pod을 죽이지 않는다.</li>
 * </ul>
 *
 * <p>즉 "모든 에러에 컨테이너 정지"가 아니라 <b>저널이 죽었을 때만</b> 정지한다.</p>
 */
@Configuration
public class AccountKafkaErrorHandlerConfig {

    @Bean
    CommonErrorHandler kafkaErrorHandler() {
        CommonDelegatingErrorHandler handler = new CommonDelegatingErrorHandler(new DefaultErrorHandler());
        // 리스너 예외는 ListenerExecutionFailedException 등으로 래핑되므로, cause 체인을 따라가
        // 실제 원인 타입(JournalUnavailableException)으로 위임 대상을 고르게 한다.
        handler.setCauseChainTraversing(true);

        CommonContainerStoppingErrorHandler stopping = new CommonContainerStoppingErrorHandler();
        stopping.setStopContainerAbnormally(true);
        handler.addDelegate(JournalUnavailableException.class, stopping);
        return handler;
    }
}
