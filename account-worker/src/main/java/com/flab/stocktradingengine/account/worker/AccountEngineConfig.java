package com.flab.stocktradingengine.account.worker;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.account.disruptor.AccountEngine;
import com.flab.stocktradingengine.account.disruptor.AccountResultListener;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

@Configuration
@EnableConfigurationProperties(AccountWorkerProperties.class)
public class AccountEngineConfig {

    private static final int BUFFER_SIZE = 1024;

    /**
     * 설정된 계좌들을 시드한 {@link AccountEngine}을 만든다. 아직 start() 는 안 부른다 — 생명주기 빈이 담당.
     *
     * <p>{@link ProducerType#MULTI}로 만든다 — 이 앱은 발행자가 하나가 아니다. account-fills
     * 컨슈머(Kafka 리스너 스레드)가 체결 반영을 발행하고, 이후 주문 접수 경로(Aeron 수신 스레드)도
     * 같은 링버퍼에 검증·예약을 발행하게 된다. SINGLE로 두면 두 스레드가 동시에 발행할 때
     * 링버퍼 시퀀스가 깨진다.</p>
     */
    @Bean
    public AccountEngine accountEngine(AccountWorkerProperties properties, AccountResultListener listener) {
        AccountEngine engine = new AccountEngine(BUFFER_SIZE, new BlockingWaitStrategy(), ProducerType.MULTI, listener);
        for (AccountWorkerProperties.SeedAccount seed : properties.seedAccounts()) {
            if (seed.holdings() == null || seed.holdings().isEmpty()) {
                engine.seed(seed.accountId(), seed.balance(), seed.marginRate());
            } else {
                engine.seed(seed.accountId(), seed.balance(), seed.marginRate(), seed.holdings());
            }
        }
        return engine;
    }

    @Bean
    public SmartLifecycle accountEngineLifecycle(AccountEngine engine) {
        return new AccountEngineLifecycle(engine);
    }
}
