package com.flab.stocktradingengine.matching.worker.lifecycle;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.SmartLifecycle;

import com.flab.stocktradingengine.matching.worker.coordination.KafkaMatchingShardAssignment;

/**
 * {@link KafkaMatchingShardAssignment}의 폴링 스레드 시작·종료를 Spring 컨텍스트 생명주기에 건다.
 * 배정이 오기 전에 들어온 주문은 담당 확인에서 거부되므로, 다른 빈보다 먼저 시작해 그 공백을 줄인다.
 */
public class KafkaMatchingShardAssignmentLifecycle implements SmartLifecycle {

    private static final int PHASE = -1;

    private final KafkaMatchingShardAssignment assignment;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public KafkaMatchingShardAssignmentLifecycle(KafkaMatchingShardAssignment assignment) {
        this.assignment = assignment;
    }

    @Override
    public void start() {
        assignment.start();
        running.set(true);
    }

    @Override
    public void stop() {
        assignment.close();
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
