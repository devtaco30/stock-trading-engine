package com.flab.stocktradingengine.matching.worker.lifecycle;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.SmartLifecycle;

import com.flab.stocktradingengine.matching.disruptor.io.AeronOrderReceiver;

/**
 * {@link AeronOrderReceiver}의 폴링 스레드 시작·종료를 Spring 컨텍스트 생명주기에 건다.
 * account-worker {@code AccountOrderReceiverLifecycle}과 같은 결.
 */
public class MatchingOrderReceiverLifecycle implements SmartLifecycle {

    private static final int PHASE = 1; // MatchingEngineLifecycle(기본 phase 0)보다 늦게 시작

    private final AeronOrderReceiver receiver;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public MatchingOrderReceiverLifecycle(AeronOrderReceiver receiver) {
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
