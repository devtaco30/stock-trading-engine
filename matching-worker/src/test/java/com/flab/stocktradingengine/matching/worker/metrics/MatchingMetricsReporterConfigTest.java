package com.flab.stocktradingengine.matching.worker.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.LifecycleProcessor;

import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.io.AeronOrderReceiver;
import com.flab.stocktradingengine.matching.worker.config.MatchingMetricsReporterConfig;
import com.flab.stocktradingengine.matching.worker.messaging.AccountFillPublisher;

/** 보고기는 부하 하네스용이라 {@code matching-worker.metrics.enabled=true}일 때만 만든다(계좌 쪽 account-worker.metrics.enabled와 같다). */
class MatchingMetricsReporterConfigTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withUserConfiguration(MatchingMetricsReporterConfig.class)
        .withBean("lifecycleProcessor", LifecycleProcessor.class, MatchingMetricsReporterConfigTest::noOpProcessor)
        .withBean(MatchingEngine.class, () -> mock(MatchingEngine.class))
        .withBean(AccountFillPublisher.class, () -> mock(AccountFillPublisher.class))
        .withBean(AeronOrderReceiver.class, () -> mock(AeronOrderReceiver.class));

    @Test
    void 스위치가_없으면_보고기_생명주기가_없다() {
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(MatchingEngineMetricsReporterLifecycle.class);
        });
    }

    @Test
    void 스위치가_켜지면_보고기_생명주기를_만든다() {
        contextRunner.withPropertyValues("matching-worker.metrics.enabled=true").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(MatchingEngineMetricsReporterLifecycle.class);
        });
    }

    private static LifecycleProcessor noOpProcessor() {
        LifecycleProcessor processor = mock(LifecycleProcessor.class);
        return processor;
    }
}
