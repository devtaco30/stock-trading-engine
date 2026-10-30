package com.flab.stocktradingengine.account.worker.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import com.flab.stocktradingengine.account.disruptor.domain.NoOpAccountResultListener;
import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.worker.listener.MetricsAccountResultListener;
import com.flab.stocktradingengine.account.worker.messaging.AeronMatchingOrderSender;

import io.aeron.Publication;

/** 매칭 샤딩 U9b — 매칭행 발신 계측 리포터는 {@code account-worker.metrics.enabled=true}일 때만 켜진다. */
class MetricsReporterConfigTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withUserConfiguration(MetricsReporterConfig.class)
        .withBean(MetricsAccountResultListener.class, MetricsAccountResultListener::new)
        .withBean(AeronMatchingOrderSender.class, () -> new AeronMatchingOrderSender(mock(Publication.class)))
        .withBean(AccountEngine.class, () -> new AccountEngine(1024, 1L, (orderId, accountId, stockCode, side, price, quantity) -> { },
            NoOpAccountResultListener.INSTANCE));

    @Test
    void 스위치가_켜져_있으면_매칭행_발신_계측_리포터가_뜬다() {
        contextRunner.withPropertyValues("account-worker.metrics.enabled=true").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasBean("matchingSendMetricsReporterLifecycle");
            assertThat(context).hasBean("accountEngineMetricsReporterLifecycle");
        });
    }

    @Test
    void 스위치가_없으면_리포터가_뜨지_않는다() {
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean("matchingSendMetricsReporterLifecycle");
            assertThat(context).doesNotHaveBean("accountEngineMetricsReporterLifecycle");
        });
    }
}
