package com.flab.stocktradingengine.account.worker.lifecycle;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.SmartLifecycle;

import com.flab.stocktradingengine.account.worker.messaging.MatchingSendMetricsReporter;

/** {@link MatchingSendMetricsReporter}의 리포터 스레드 시작·종료를 Spring 컨텍스트 생명주기에 건다. */
public class MatchingSendMetricsReporterLifecycle implements SmartLifecycle {

    private static final int PHASE = 1; // AccountEngineLifecycle(기본 phase 0)보다 늦게 시작

    private final MatchingSendMetricsReporter reporter;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public MatchingSendMetricsReporterLifecycle(MatchingSendMetricsReporter reporter) {
        this.reporter = reporter;
    }

    @Override
    public void start() {
        reporter.start();
        running.set(true);
    }

    @Override
    public void stop() {
        reporter.close();
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
