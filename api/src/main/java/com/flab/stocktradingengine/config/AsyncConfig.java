package com.flab.stocktradingengine.config;

import java.util.concurrent.Executor;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * 비동기 처리 설정.
 *
 * <p>{@code @EnableAsync} 로 스프링의 {@code @Async} 기능을 활성화한다.
 * 기본 SimpleAsyncTaskExecutor 는 매 요청마다 스레드를 생성해 비효율적이므로
 * 전용 스레드 풀({@code matchingFillExecutor})을 정의한다.</p>
 *
 * <h3>matchingFillExecutor 설정 근거</h3>
 * <ul>
 *   <li>{@code corePoolSize=4}: 평시 동시 체결 DB 쓰기 수. 대부분의 경우 여기서 처리됨.</li>
 *   <li>{@code maxPoolSize=16}: 체결이 폭발적으로 몰릴 때 최대 확장 크기.</li>
 *   <li>{@code queueCapacity=1000}: 풀이 가득 찼을 때 대기할 수 있는 이벤트 수.
 *       초과하면 CallerRunsPolicy 에 따라 매칭 스레드가 직접 처리한다.</li>
 * </ul>
 */
@Configuration
@EnableAsync
public class AsyncConfig {

    /**
     * 체결 DB 반영 전용 스레드 풀.
     * {@code MatchingFillHandler} 의 {@code @Async("matchingFillExecutor")} 에서 사용.
     */
    @Bean(name = "matchingFillExecutor")
    public Executor matchingFillExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(16);
        executor.setQueueCapacity(1000);
        executor.setThreadNamePrefix("fill-handler-");
        // 큐와 풀이 모두 가득 차면 이벤트를 발행한 스레드(매칭 스레드)가 직접 처리.
        // 완전 거부보다 처리 속도를 줄이더라도 체결을 보장하는 정책.
        executor.setRejectedExecutionHandler(new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }
}
