package com.flab.stocktradingengine.account.worker.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.account.worker.lifecycle.MetricsReporterLifecycle;
import com.flab.stocktradingengine.account.worker.listener.MetricsAccountResultListener;

/**
 * {@link MetricsAccountResultListener}는 {@code @Component}로 자동 등록돼
 * {@code List<AccountResultListener>}에 자동으로 잡히므로(카운터 증가는 항상 켜짐, 비용이
 * LongAdder increment뿐이라 무해), 이 config는 리포터 스레드(1초마다 로그) 시작·종료를 Spring
 * 생명주기에 거는 배선만 담당한다 — {@code account-worker.metrics.enabled=true}일 때만 이
 * 빈이 생겨 리포터 스레드가 뜬다. 부하 하네스 전용 로그라 데모·기본 udp 실행에선 조용히 있어야
 * 한다(39 리뷰 지적). {@link AccountStatePublisherConfig}와 같은 결.
 */
@Configuration
public class MetricsReporterConfig {

    @Bean
    @ConditionalOnProperty(prefix = "account-worker.metrics", name = "enabled", havingValue = "true")
    public SmartLifecycle metricsReporterLifecycle(MetricsAccountResultListener listener) {
        return new MetricsReporterLifecycle(listener);
    }
}
