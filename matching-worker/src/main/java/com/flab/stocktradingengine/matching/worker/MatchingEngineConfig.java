package com.flab.stocktradingengine.matching.worker;

import java.util.List;

import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.matching.disruptor.Journal;
import com.flab.stocktradingengine.matching.disruptor.MatchListener;
import com.flab.stocktradingengine.matching.disruptor.MatchingEngine;
import com.lmax.disruptor.BlockingWaitStrategy;

@Configuration
public class MatchingEngineConfig {

    private static final int BUFFER_SIZE = 1024;

    /**
     * 발행자가 하나(테스트 스레드 또는 이후 Aeron 수신 스레드)라 ProducerType.SINGLE(기본값)로 충분하다.
     * 아직 start()는 안 부른다 — 생명주기 빈이 담당.
     *
     * <p>저널은 기본(인메모리) 대신 {@link MatchingJournalArchiveConfig}가 만든 Aeron Archive durable
     * 구현({@code AeronArchiveMatchingJournal})을 명시적으로 넘긴다(2c-1) — 프로세스가 죽어도
     * 저널이 디스크에 남아야 2c-2 리플레이가 성립한다.</p>
     *
     * <p>{@link MatchingJournalArchiveConfig#matchingJournalRecoveredEntries}(이전 녹화를 읽어둔
     * 결과)를 start() 전에 재적용한다(2c-2) — 재시작 전 호가창을 되살린 뒤에야 라이브 트래픽을
     * 받는다. 계좌 축과 달리 시드할 상태가 없어(호가창은 주문 리플레이만으로 전부 재구성된다)
     * seed 단계는 없다.</p>
     */
    @Bean
    public MatchingEngine matchingEngine(MatchListener listener, Journal journal, List<JournaledOrder> matchingJournalRecoveredEntries) {
        MatchingEngine engine = new MatchingEngine(BUFFER_SIZE, new BlockingWaitStrategy(), listener, journal);
        engine.recover(matchingJournalRecoveredEntries);
        return engine;
    }

    @Bean
    public SmartLifecycle matchingEngineLifecycle(MatchingEngine engine) {
        return new MatchingEngineLifecycle(engine);
    }
}
