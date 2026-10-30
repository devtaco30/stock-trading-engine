package com.flab.stocktradingengine.matching.worker.metrics;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.SmartLifecycle;

/** {@link MatchingEngineMetricsReporter}의 보고 thread 시작·종료를 Spring 컨텍스트 생명주기에 건다. */
public class MatchingEngineMetricsReporterLifecycle implements SmartLifecycle {

    private final MatchingEngineMetricsReporter reporter;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public MatchingEngineMetricsReporterLifecycle(MatchingEngineMetricsReporter reporter) {
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
}
