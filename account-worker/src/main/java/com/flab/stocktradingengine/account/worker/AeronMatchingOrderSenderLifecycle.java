package com.flab.stocktradingengine.account.worker;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.SmartLifecycle;

/**
 * {@link AeronMatchingOrderSender}의 publisher 스레드 시작·종료를 Spring 컨텍스트 생명주기에 건다.
 * {@link AccountOrderReceiverLifecycle}과 같은 결.
 */
class AeronMatchingOrderSenderLifecycle implements SmartLifecycle {

    private static final int PHASE = 1; // AccountEngineLifecycle(기본 phase 0)보다 늦게 시작

    private final AeronMatchingOrderSender sender;
    private final AtomicBoolean running = new AtomicBoolean(false);

    AeronMatchingOrderSenderLifecycle(AeronMatchingOrderSender sender) {
        this.sender = sender;
    }

    @Override
    public void start() {
        sender.start();
        running.set(true);
    }

    @Override
    public void stop() {
        sender.close();
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
