package com.flab.stocktradingengine.matching.worker.lifecycle;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.SmartLifecycle;

/**
 * 호가창 이동 thread(요청 응답 thread, 수신 thread)와 맵 읽기의 시작·종료를 Spring 생명주기에 건다. 시작과 종료 동작을 받아
 * 여러 구성요소가 같은 클래스를 쓴다.
 */
public class HandoverLifecycle implements SmartLifecycle {

    private final int phase;
    private final Runnable startAction;
    private final Runnable stopAction;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public HandoverLifecycle(int phase, Runnable startAction, Runnable stopAction) {
        this.phase = phase;
        this.startAction = startAction;
        this.stopAction = stopAction;
    }

    @Override
    public void start() {
        startAction.run();
        running.set(true);
    }

    @Override
    public void stop() {
        stopAction.run();
        running.set(false);
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return phase;
    }
}
