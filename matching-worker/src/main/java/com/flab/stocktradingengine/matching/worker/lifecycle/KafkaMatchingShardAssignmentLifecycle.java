package com.flab.stocktradingengine.matching.worker.lifecycle;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.SmartLifecycle;

import com.flab.stocktradingengine.matching.worker.coordination.KafkaMatchingShardAssignment;

/**
 * {@link KafkaMatchingShardAssignment}의 폴링 스레드 시작·종료를 Spring 컨텍스트 생명주기에 건다.
 * 배정이 오기 전에 들어온 주문은 담당 확인에서 거부되므로, 다른 빈보다 먼저 시작해 그 공백을 줄인다.
 */
public class KafkaMatchingShardAssignmentLifecycle implements SmartLifecycle {

    // 멈출 때는 phase가 높은 것부터 멈춘다. 스냅샷 생명주기(MatchingSnapshotLifecycle, phase -1)가 먼저 멈춰
    // 담당 종목 slot 목록을 스냅샷에 쓴 뒤에 이 생명주기가 멈추도록 -2로 둔다. 컨슈머를 닫으면 회수 콜백이 담당을 전부 끄기 때문이다.
    // 시작할 때는 낮은 phase가 먼저 시작하므로 다른 빈보다 먼저 시작한다는 의도는 그대로다.
    private static final int PHASE = -2;

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
