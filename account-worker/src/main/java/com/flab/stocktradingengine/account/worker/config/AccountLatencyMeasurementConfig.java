package com.flab.stocktradingengine.account.worker.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.account.worker.lifecycle.AccountLatencyDumpLifecycle;
import com.flab.stocktradingengine.time.LatencyHistogram;
import com.flab.stocktradingengine.time.StageDumpRunner;
import com.flab.stocktradingengine.time.StageLatencyRegistry;

/**
 * 끝점①(접수·예약) 접수 지연 측정(decision_records/v1-v2-e2e-measurement.md)의 배선.
 *
 * <p>{@link LatencyHistogram} 빈은 {@code measure.latency.enabled} 값과 무관하게 항상 만든다 —
 * {@link AccountEngineConfig}가 {@link com.flab.stocktradingengine.account.disruptor.engine.AccountEngine}
 * 생성자에 그대로 주입해야 하고(끄면 record가 즉시 반환하는 no-op이라 항상 있어도 무해), 그래야
 * 꺼져 있을 때 별도 null 체크 분기가 생기지 않는다. 반면 파일로 내보내는 {@link
 * AccountLatencyDumpLifecycle}(폴링 스레드·파일 I/O)은 켜져 있을 때만 만든다 — 데모·기본 실행에선
 * 조용히 있어야 한다({@link MetricsReporterConfig}와 같은 결).</p>
 */
@Configuration
public class AccountLatencyMeasurementConfig {

    @Bean
    public LatencyHistogram accountLatencyHistogram(@Value("${measure.latency.enabled:false}") boolean enabled) {
        return new LatencyHistogram(enabled);
    }

    @Bean
    @ConditionalOnProperty(prefix = "measure.latency", name = "enabled", havingValue = "true")
    public SmartLifecycle accountLatencyDumpLifecycle(
            LatencyHistogram accountLatencyHistogram,
            @Value("${measure.latency.output-path:loadtest/results/latency-v2-account-worker.json}") String outputPath,
            @Value("${measure.latency.snapshot-trigger-path:}") String snapshotTriggerPath) {
        return new AccountLatencyDumpLifecycle(accountLatencyHistogram, outputPath, snapshotTriggerPath);
    }

    /**
     * 구간 분해 측정(measure/segments 브랜치 한정)의 단계별 지연 등록부. {@code measure.stages.enabled}가
     * 꺼져 있으면(기본값) 어떤 값도 기록하지 않고, {@link StageDumpRunner}도 스레드와 파일을 만들지 않는다.
     */
    @Bean
    public StageLatencyRegistry accountStageLatencyRegistry(@Value("${measure.stages.enabled:false}") boolean enabled) {
        return new StageLatencyRegistry(enabled);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public StageDumpRunner accountStageDumpRunner(
            StageLatencyRegistry accountStageLatencyRegistry,
            @Value("${measure.stages.output-path:loadtest/results/stages-v2-account-worker.json}") String outputPath,
            @Value("${measure.stages.snapshot-trigger-path:}") String snapshotTriggerPath,
            @Value("${measure.stages.series-stage:B}") String seriesStage,
            @Value("${measure.stages.series-path:}") String seriesPath) {
        return new StageDumpRunner(accountStageLatencyRegistry, outputPath, snapshotTriggerPath, seriesStage, seriesPath);
    }
}
