package com.flab.stocktradingengine.matching.worker;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.SmartLifecycle;

import com.flab.stocktradingengine.matching.disruptor.MatchingEngine;

/**
 * {@link MatchingEngine}의 시작·종료를 Spring 컨텍스트 생명주기에 건다.
 * account-worker의 {@code AccountEngineLifecycle}과 같은 결.
 */
class MatchingEngineLifecycle implements SmartLifecycle {

    private final MatchingEngine engine;
    private final AtomicBoolean running = new AtomicBoolean(false);

    MatchingEngineLifecycle(MatchingEngine engine) {
        this.engine = engine;
    }

    @Override
    public void start() {
        engine.start();
        running.set(true);
    }

    @Override
    public void stop() {
        engine.shutdown();
        running.set(false);
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }
}
