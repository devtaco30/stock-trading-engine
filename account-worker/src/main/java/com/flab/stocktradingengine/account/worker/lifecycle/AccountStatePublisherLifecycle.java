package com.flab.stocktradingengine.account.worker.lifecycle;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.SmartLifecycle;

import com.flab.stocktradingengine.account.worker.messaging.AccountStatePublisher;

/**
 * {@link AccountStatePublisher}의 publisher 스레드 시작·종료를 Spring 컨텍스트 생명주기에 건다.
 * {@link AeronMatchingOrderSenderLifecycle}과 같은 결.
 */
public class AccountStatePublisherLifecycle implements SmartLifecycle {

    private static final int PHASE = 1; // AccountEngineLifecycle(기본 phase 0)보다 늦게 시작

    private final AccountStatePublisher publisher;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public AccountStatePublisherLifecycle(AccountStatePublisher publisher) {
        this.publisher = publisher;
    }

    @Override
    public void start() {
        publisher.start();
        running.set(true);
    }

    @Override
    public void stop() {
        publisher.close();
        running.set(false);
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return PHASE;
    }
}
