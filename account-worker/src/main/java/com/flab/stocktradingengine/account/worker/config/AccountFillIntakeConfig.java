package com.flab.stocktradingengine.account.worker.config;

import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.disruptor.io.AccountFillReceiver;
import com.flab.stocktradingengine.account.worker.lifecycle.AccountFillReceiverLifecycle;

import io.aeron.Aeron;
import io.aeron.Subscription;

/**
 * ADR-032, U3 — 매칭이 Aeron으로 발행하는 체결을 받는 인테이크 경로 배선. {@link AccountOrderIntakeConfig}가
 * 만든 account-worker 자체 Aeron(MediaDriver)을 그대로 써서 Subscription만 새로 연다(그 결과
 * 클래스와 같은 결). Kafka {@code AccountFillConsumer}를 대체한다 — 정산 컨슈머(account-settlements)는
 * 그대로 Kafka에 남는다({@link AccountKafkaErrorHandlerConfig} 참고).
 *
 * <p>채널·스트림은 matching-worker의 {@code MatchingFillPublishConfig.FILL_STREAM_ID}(6001)와
 * 반드시 같아야 한다 — account-worker는 모듈 경계상 matching-worker에 의존하지 않아 상수를
 * 공유하지 못하고 값만 맞춘다({@code MatchingOrderSenderConfig}와 같은 이유).</p>
 */
@Configuration
public class AccountFillIntakeConfig {

    // 패키지 가시성 — 테스트(같은 패키지)가 이 값을 그대로 참조해 matching-worker 체결 스트림과 맞춘다.
    static final String FILL_CHANNEL = "aeron:ipc";
    static final int FILL_STREAM_ID = 6001; // matching-worker MatchingFillPublishConfig.FILL_STREAM_ID와 동일해야 함

    @Bean(destroyMethod = "close")
    public Subscription accountFillSubscription(Aeron aeron) {
        return aeron.addSubscription(FILL_CHANNEL, FILL_STREAM_ID);
    }

    @Bean
    public AccountFillReceiver accountFillReceiver(Subscription accountFillSubscription, AccountEngine accountEngine) {
        return new AccountFillReceiver(accountFillSubscription, accountEngine);
    }

    /**
     * {@link AccountEngineConfig#accountEngineLifecycle}보다 늦은 phase로 둔다 —
     * {@link AccountOrderIntakeConfig#accountOrderReceiverLifecycle}과 같은 이유.
     */
    @Bean
    public SmartLifecycle accountFillReceiverLifecycle(AccountFillReceiver accountFillReceiver) {
        return new AccountFillReceiverLifecycle(accountFillReceiver);
    }
}
