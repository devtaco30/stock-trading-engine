package com.flab.stocktradingengine.matching.worker.lifecycle;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.SmartLifecycle;

import com.flab.stocktradingengine.aeron.AssignmentDestinationResolver;

/** {@link AssignmentDestinationResolver}의 폴링 스레드 시작·종료를 Spring 컨텍스트 생명주기에 건다. */
public class AssignmentDestinationResolverLifecycle implements SmartLifecycle {

    private final AssignmentDestinationResolver resolver;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public AssignmentDestinationResolverLifecycle(AssignmentDestinationResolver resolver) {
        this.resolver = resolver;
    }

    @Override
    public void start() {
        resolver.start();
        running.set(true);
    }

    @Override
    public void stop() {
        resolver.close();
        running.set(false);
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }
}
