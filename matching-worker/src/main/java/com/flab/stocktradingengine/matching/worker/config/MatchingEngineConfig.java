package com.flab.stocktradingengine.matching.worker.config;

import java.util.List;
import java.util.Optional;

import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.io.MatchListener;
import com.flab.stocktradingengine.matching.disruptor.journal.Journal;
import com.flab.stocktradingengine.matching.worker.lifecycle.MatchingEngineLifecycle;
import com.flab.stocktradingengine.matching.worker.recovery.StoredMatchingSnapshot;
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
     * <p>스냅샷이 있으면(2d-1b) 먼저 그걸로 호가창을 복원한 뒤, 그 스냅샷 이후분(delta)만 담긴
     * {@link MatchingJournalArchiveConfig#matchingJournalRecoveredEntries}를 재적용한다. 스냅샷이
     * 없으면(2c-2, 하위호환) 저널 전체가 그대로 recover 입력이 된다. 계좌 축과 달리 시드할 상태가
     * 없어(호가창은 주문 리플레이만으로 전부 재구성된다) seed 단계는 없다.</p>
     */
    @Bean
    public MatchingEngine matchingEngine(MatchListener listener, Journal journal,
            Optional<StoredMatchingSnapshot> matchingLoadedSnapshot, List<JournaledOrder> matchingJournalRecoveredEntries) {
        MatchingEngine engine = new MatchingEngine(BUFFER_SIZE, new BlockingWaitStrategy(), listener, journal);
        matchingLoadedSnapshot.ifPresent(stored -> engine.restore(stored.snapshot()));
        engine.recover(matchingJournalRecoveredEntries);
        return engine;
    }

    @Bean
    public SmartLifecycle matchingEngineLifecycle(MatchingEngine engine) {
        return new MatchingEngineLifecycle(engine);
    }
}
