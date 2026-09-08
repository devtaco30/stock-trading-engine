package com.flab.stocktradingengine.account.worker;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.SmartLifecycle;

import com.flab.stocktradingengine.account.disruptor.AccountEngine;

/**
 * {@link AccountEngine}의 시작·종료를 Spring 컨텍스트 생명주기에 건다.
 *
 * <p>컨텍스트가 뜨면(모든 빈이 준비된 뒤) {@link #start()}가 불려 엔진 소비자 스레드가 기동되고,
 * {@code SpringApplication.run()}이 등록하는 종료 훅이 컨텍스트를 닫을 때 {@link #stop()}이 불려
 * {@code engine.shutdown()}까지 정상 호출된다. 별도로 대기 스레드·shutdown hook을 짤 필요가 없다.</p>
 */
class AccountEngineLifecycle implements SmartLifecycle {

    private final AccountEngine engine;
    private final AtomicBoolean running = new AtomicBoolean(false);

    AccountEngineLifecycle(AccountEngine engine) {
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
