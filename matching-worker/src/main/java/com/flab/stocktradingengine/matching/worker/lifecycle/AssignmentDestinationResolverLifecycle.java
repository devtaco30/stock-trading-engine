package com.flab.stocktradingengine.matching.worker.lifecycle;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.SmartLifecycle;

import com.flab.stocktradingengine.aeron.AssignmentDestinationResolver;

/**
 * {@link AssignmentDestinationResolver}의 폴링 스레드 시작·종료를 Spring 컨텍스트 생명주기에 건다.
 *
 * <h3>초기 읽기가 끝날 때까지 기동을 막는다(계좌 샤딩 U6)</h3>
 * <p>{@link #start()}가 {@link AssignmentDestinationResolver#awaitInitialCatchUp}으로 블로킹돼,
 * 이 빈이 속한 phase(기본 0)가 안 끝난다 — {@code MatchingOrderReceiverLifecycle}(phase 1)은
 * Spring이 phase 0 전체가 끝난 뒤에야 시작하므로, 매칭이 주문 인테이크를 열 때는 이미 그 시점까지
 * 쌓인 account-shard-map 배정을 다 안다. "아직 아무 배정도 모르는 채로 체결이 나서 목적지를
 * 못 찾는" 상황을 재시도가 아니라 기동 순서로 막는다({@link
 * com.flab.stocktradingengine.matching.worker.messaging.AccountDestinationMisconfiguredException}과
 * 다른 문제 — 이건 "아직 안 왔다", 그건 "설정이 잘못됐다").</p>
 */
public class AssignmentDestinationResolverLifecycle implements SmartLifecycle {

    private static final Duration INITIAL_CATCH_UP_TIMEOUT = Duration.ofSeconds(30);

    private final AssignmentDestinationResolver resolver;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public AssignmentDestinationResolverLifecycle(AssignmentDestinationResolver resolver) {
        this.resolver = resolver;
    }

    @Override
    public void start() {
        resolver.start();
        resolver.awaitInitialCatchUp(INITIAL_CATCH_UP_TIMEOUT);
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
