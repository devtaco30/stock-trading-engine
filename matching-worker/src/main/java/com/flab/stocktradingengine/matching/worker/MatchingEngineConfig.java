package com.flab.stocktradingengine.matching.worker;

import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.matching.disruptor.Journal;
import com.flab.stocktradingengine.matching.disruptor.MatchListener;
import com.flab.stocktradingengine.matching.disruptor.MatchingEngine;
import com.lmax.disruptor.BlockingWaitStrategy;

@Configuration
public class MatchingEngineConfig {

    private static final int BUFFER_SIZE = 1024;

    /**
     * 발행자가 하나(테스트 스레드 또는 이후 Aeron 수신 스레드)라 ProducerType.SINGLE(기본값)로 충분하다.
     *
     * <p>저널은 기본(인메모리) 대신 {@link MatchingJournalArchiveConfig}가 만든 Aeron Archive durable
     * 구현({@code AeronArchiveMatchingJournal})을 명시적으로 넘긴다(2c-1) — 프로세스가 죽어도
     * 저널이 디스크에 남아야 2c-2 리플레이가 성립한다.</p>
     */
    @Bean
    public MatchingEngine matchingEngine(MatchListener listener, Journal journal) {
        return new MatchingEngine(BUFFER_SIZE, new BlockingWaitStrategy(), listener, journal);
    }

    @Bean
    public SmartLifecycle matchingEngineLifecycle(MatchingEngine engine) {
        return new MatchingEngineLifecycle(engine);
    }
}
