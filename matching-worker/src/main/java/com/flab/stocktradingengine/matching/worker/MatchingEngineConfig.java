package com.flab.stocktradingengine.matching.worker;

import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.matching.disruptor.MatchListener;
import com.flab.stocktradingengine.matching.disruptor.MatchingEngine;

@Configuration
public class MatchingEngineConfig {

    private static final int BUFFER_SIZE = 1024;

    /** 발행자가 하나(테스트 스레드 또는 이후 Aeron 수신 스레드)라 ProducerType.SINGLE(기본값)로 충분하다. */
    @Bean
    public MatchingEngine matchingEngine(MatchListener listener) {
        return new MatchingEngine(BUFFER_SIZE, listener);
    }

    @Bean
    public SmartLifecycle matchingEngineLifecycle(MatchingEngine engine) {
        return new MatchingEngineLifecycle(engine);
    }
}
