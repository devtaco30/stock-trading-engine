package com.flab.stocktradingengine.api.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.time.StageLatencyRegistry;

/**
 * 구간 분해 측정(measure/segments 브랜치 한정)의 api 쪽 단계별 지연 등록부.
 * {@code measure.stages.enabled}가 꺼져 있으면(기본값) 등록부는 어떤 값도 기록하지 않는다.
 */
@Configuration
public class ApiStageMeasurementConfig {

    @Bean
    public StageLatencyRegistry apiStageLatencyRegistry(@Value("${measure.stages.enabled:false}") boolean enabled) {
        return new StageLatencyRegistry(enabled);
    }
}
