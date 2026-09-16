package com.flab.stocktradingengine.account.worker.lifecycle;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.SmartLifecycle;

import com.flab.stocktradingengine.account.worker.listener.MetricsAccountResultListener;

/**
 * {@link MetricsAccountResultListener}의 리포터 스레드 시작·종료를 Spring 컨텍스트 생명주기에 건다.
 * {@link AccountStatePublisherLifecycle}과 같은 결.
 */
public class MetricsReporterLifecycle implements SmartLifecycle {

    private static final int PHASE = 1; // AccountEngineLifecycle(기본 phase 0)보다 늦게 시작

    private final MetricsAccountResultListener listener;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public MetricsReporterLifecycle(MetricsAccountResultListener listener) {
        this.listener = listener;
    }

    @Override
    public void start() {
        listener.start();
        running.set(true);
    }

    @Override
    public void stop() {
        listener.close();
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
