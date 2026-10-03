package com.flab.stocktradingengine.api.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.flab.stocktradingengine.aeron.AccountDestinationResolver;
import com.flab.stocktradingengine.api.messaging.AccountOrderDispatcher;
import com.flab.stocktradingengine.api.messaging.AeronAccountOrderSender;
import com.flab.stocktradingengine.api.messaging.OrderDeliveryFailureStore;
import com.flab.stocktradingengine.api.messaging.RedisOrderDeliveryFailureStore;

/**
 * v2 주문 전달 큐와 전용 스레드, 전달 실패 기록 장소를 등록한다. 큐 용량과 마감 시각은 데이터로 정한 값이 아니라 탐색용 임의 값이다.
 */
@Configuration
public class AccountOrderDispatchConfig {

    @Bean
    public OrderDeliveryFailureStore orderDeliveryFailureStore(StringRedisTemplate stringRedisTemplate) {
        return new RedisOrderDeliveryFailureStore(stringRedisTemplate);
    }

    @Bean(initMethod = "start", destroyMethod = "close")
    public AccountOrderDispatcher accountOrderDispatcher(
            AccountDestinationResolver accountDestinationResolver, AeronAccountOrderSender aeronAccountOrderSender,
            OrderDeliveryFailureStore orderDeliveryFailureStore,
            @Value("${api.order-dispatch.queue-capacity:65536}") int queueCapacity,
            @Value("${api.order-dispatch.deadline-millis:2000}") long deadlineMillis) {
        return new AccountOrderDispatcher(
            accountDestinationResolver, aeronAccountOrderSender, orderDeliveryFailureStore, queueCapacity, deadlineMillis);
    }
}
