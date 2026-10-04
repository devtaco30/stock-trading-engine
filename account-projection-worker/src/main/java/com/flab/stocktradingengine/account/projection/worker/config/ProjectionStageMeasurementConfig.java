package com.flab.stocktradingengine.account.projection.worker.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.time.StageDumpRunner;
import com.flab.stocktradingengine.time.StageLatencyRegistry;

/**
 * 구간 분해 측정(measure/segments 브랜치 한정)의 account-projection-worker 쪽 단계별 지연 등록부와 덤프 실행기.
 * 설정 키는 다른 프로세스와 같다. {@code measure.stages.enabled}가 꺼져 있으면(기본값) 어떤 값도 기록하지 않고
 * 실행기도 스레드와 파일을 만들지 않는다.
 */
@Configuration
public class ProjectionStageMeasurementConfig {

    @Bean
    public StageLatencyRegistry projectionStageLatencyRegistry(@Value("${measure.stages.enabled:false}") boolean enabled) {
        return new StageLatencyRegistry(enabled);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public StageDumpRunner projectionStageDumpRunner(
            StageLatencyRegistry projectionStageLatencyRegistry,
            @Value("${measure.stages.output-path:loadtest/results/stages-v2-account-projection-worker.json}") String outputPath,
            @Value("${measure.stages.snapshot-trigger-path:}") String snapshotTriggerPath,
            @Value("${measure.stages.series-stage:}") String seriesStage,
            @Value("${measure.stages.series-path:}") String seriesPath) {
        return new StageDumpRunner(projectionStageLatencyRegistry, outputPath, snapshotTriggerPath, seriesStage, seriesPath);
    }
}
