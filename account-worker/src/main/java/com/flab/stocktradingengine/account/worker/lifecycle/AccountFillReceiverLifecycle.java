package com.flab.stocktradingengine.account.worker.lifecycle;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.SmartLifecycle;

import com.flab.stocktradingengine.account.disruptor.io.AccountFillReceiver;

/**
 * {@link AccountFillReceiver}의 폴링 스레드 시작·종료를 Spring 컨텍스트 생명주기에 건다.
 * {@link AccountOrderReceiverLifecycle}과 같은 결(ADR-032, U3).
 */
public class AccountFillReceiverLifecycle implements SmartLifecycle {

    private static final int PHASE = 1; // AccountEngineLifecycle(기본 phase 0)보다 늦게 시작

    private final AccountFillReceiver receiver;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public AccountFillReceiverLifecycle(AccountFillReceiver receiver) {
        this.receiver = receiver;
    }

    @Override
    public void start() {
        receiver.start();
        running.set(true);
    }

    @Override
    public void stop() {
        receiver.close();
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
