package com.flab.stocktradingengine.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.time.StageDumpRunner;
import com.flab.stocktradingengine.time.StageLatencyRegistry;

/**
 * 구간 분해 측정(measure/segments 브랜치 한정)의 v1 order-engine 단계별 지연 등록부와 덤프 실행기.
 * 설정 키는 v2 프로세스와 같다. {@code measure.stages.enabled}가 꺼져 있으면(기본값) 아무것도 기록하지 않는다.
 */
@Configuration
public class OrderStageMeasurementConfig {

    @Bean
    public StageLatencyRegistry orderStageLatencyRegistry(@Value("${measure.stages.enabled:false}") boolean enabled) {
        return new StageLatencyRegistry(enabled);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public StageDumpRunner orderStageDumpRunner(
            StageLatencyRegistry orderStageLatencyRegistry,
            @Value("${measure.stages.output-path:loadtest/results/stages-v1-order-engine.json}") String outputPath,
            @Value("${measure.stages.snapshot-trigger-path:}") String snapshotTriggerPath,
            @Value("${measure.stages.series-stage:B}") String seriesStage,
            @Value("${measure.stages.series-path:}") String seriesPath) {
        return new StageDumpRunner(orderStageLatencyRegistry, outputPath, snapshotTriggerPath, seriesStage, seriesPath);
    }
}
