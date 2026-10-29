package com.flab.stocktradingengine.account.worker.lifecycle;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.SmartLifecycle;

import com.flab.stocktradingengine.account.worker.coordination.MatchingDestinationResolver;

/**
 * {@link MatchingDestinationResolver}의 폴링 스레드 시작·종료를 Spring 컨텍스트 생명주기에 건다.
 *
 * <h3>초기 읽기가 끝날 때까지 주문 접수를 열지 않는다</h3>
 * <p>{@link #start()}가 {@code awaitInitialCatchUp}으로 블로킹돼 이 빈이 속한 phase(0)가 안 끝난다.
 * 주문 접수({@link AccountOrderReceiverLifecycle})와 매칭행 발신 스레드({@link AeronMatchingOrderSenderLifecycle})는
 * phase 1이라 Spring이 phase 0 전체가 끝난 뒤에야 시작한다. 계좌 shard가 "아직 아무 담당도 모르는 채로"
 * 주문을 받는 일을 재시도가 아니라 기동 순서로 막는다. phase를 명시한 이유: {@link SmartLifecycle}의
 * 기본 phase는 가장 늦은 값이라 명시하지 않으면 접수보다 나중에 시작한다.</p>
 */
public class MatchingDestinationResolverLifecycle implements SmartLifecycle {

    private static final int PHASE = 0;
    private static final Duration INITIAL_CATCH_UP_TIMEOUT = Duration.ofSeconds(30);

    private final MatchingDestinationResolver resolver;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public MatchingDestinationResolverLifecycle(MatchingDestinationResolver resolver) {
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

    @Override
    public int getPhase() {
        return PHASE;
    }
}
