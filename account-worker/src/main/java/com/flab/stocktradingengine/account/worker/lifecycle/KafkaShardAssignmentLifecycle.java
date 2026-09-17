package com.flab.stocktradingengine.account.worker.lifecycle;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.SmartLifecycle;

import com.flab.stocktradingengine.account.worker.coordination.KafkaShardAssignment;

/**
 * {@link KafkaShardAssignment}의 폴링 스레드 시작·종료를 Spring 컨텍스트 생명주기에 건다.
 *
 * <p>{@link AccountEngineLifecycle}(기본 phase 0)보다 먼저 시작해, 계좌 엔진이 뜨기 전부터 최대한
 * 빨리 조정 토픽 배정을 받게 한다 — 배정이 늦게 와도 그 사이엔 NOT_OWNED로 거부될 뿐이라 안전하지만,
 * 먼저 시작해 두면 그 공백이 그만큼 짧아진다.</p>
 */
public class KafkaShardAssignmentLifecycle implements SmartLifecycle {

    private static final int PHASE = -1;

    private final KafkaShardAssignment assignment;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public KafkaShardAssignmentLifecycle(KafkaShardAssignment assignment) {
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
