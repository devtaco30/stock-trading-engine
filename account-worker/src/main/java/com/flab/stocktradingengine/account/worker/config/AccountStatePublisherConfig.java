package com.flab.stocktradingengine.account.worker.config;

import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.account.worker.lifecycle.AccountStatePublisherLifecycle;
import com.flab.stocktradingengine.account.worker.messaging.AccountStatePublisher;

/**
 * {@link AccountStatePublisher}는 {@code @Component}로 자동 등록돼 {@code List<AccountResultListener>}에
 * 자동으로 잡히므로(계좌 상태 영속/프로젝션 트랙 Unit 2), 이 config는 publisher 스레드 시작·종료를
 * Spring 생명주기에 거는 배선만 담당한다. {@link MatchingOrderSenderConfig}와 같은 결.
 */
@Configuration
public class AccountStatePublisherConfig {

    @Bean
    public SmartLifecycle accountStatePublisherLifecycle(AccountStatePublisher accountStatePublisher) {
        return new AccountStatePublisherLifecycle(accountStatePublisher);
    }
}
