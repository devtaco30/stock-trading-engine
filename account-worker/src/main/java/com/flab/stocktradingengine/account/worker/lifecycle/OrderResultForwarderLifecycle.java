package com.flab.stocktradingengine.account.worker.lifecycle;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.SmartLifecycle;

import com.flab.stocktradingengine.account.worker.messaging.OrderResultForwarder;

/**
 * {@link OrderResultForwarder}의 폴링 스레드 시작·종료를 Spring 컨텍스트 생명주기에 건다.
 * {@link AccountFillReceiverLifecycle}과 같은 결.
 */
public class OrderResultForwarderLifecycle implements SmartLifecycle {

    private static final int PHASE = 1; // AccountEngineLifecycle(기본 phase 0)보다 늦게 시작

    private final OrderResultForwarder forwarder;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public OrderResultForwarderLifecycle(OrderResultForwarder forwarder) {
        this.forwarder = forwarder;
    }

    @Override
    public void start() {
        forwarder.start();
        running.set(true);
    }

    @Override
    public void stop() {
        forwarder.close();
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
