package com.flab.stocktradingengine.matching.worker.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.io.AeronOrderReceiver;
import com.flab.stocktradingengine.matching.worker.messaging.AccountFillPublisher;
import com.flab.stocktradingengine.matching.worker.messaging.RejectReplyPublisher;
import com.flab.stocktradingengine.matching.worker.metrics.MatchingEngineMetricsReporter;
import com.flab.stocktradingengine.matching.worker.metrics.MatchingEngineMetricsReporterLifecycle;

/**
 * 매칭 엔진의 ring buffer, 저널 offer, 체결 발신 queue 계측(12-3절 2번 표의 7·8·9단계)을 1초마다 로그 한 줄로 내는 보고기를 건다.
 * 부하 하네스 전용 로그라 {@code matching-worker.metrics.enabled=true}일 때만 만든다(계좌 쪽 account-worker.metrics.enabled와 같다).
 */
@Configuration
public class MatchingMetricsReporterConfig {

    @Bean
    @ConditionalOnProperty(prefix = "matching-worker.metrics", name = "enabled", havingValue = "true")
    public SmartLifecycle matchingEngineMetricsReporterLifecycle(
            MatchingEngine engine, AccountFillPublisher fillPublisher, AeronOrderReceiver orderReceiver,
            RejectReplyPublisher rejectReplyPublisher) {
        return new MatchingEngineMetricsReporterLifecycle(
            new MatchingEngineMetricsReporter(engine, fillPublisher::fillQueueDepth, fillPublisher::fillEnqueueWaitCount,
                orderReceiver::placeAcceptedCount, orderReceiver::notOwnedCount, rejectReplyPublisher::sentCount));
    }
}
