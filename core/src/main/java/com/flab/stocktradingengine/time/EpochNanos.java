package com.flab.stocktradingengine.time;

import java.time.Instant;

/**
 * 프로세스 간 비교 가능한 시각값 — wall-clock epoch 나노초(long).
 *
 * <p>{@code System.nanoTime()}은 JVM 내부 상대값이라 서로 다른 프로세스(v1의 order-engine,
 * v2의 account-worker 등)가 찍은 값끼리 뺄셈이 성립하지 않는다. {@link Instant}는 절대 UTC
 * 기반이라 프로세스가 달라도 비교 가능하지만 기본 API가 초·나노 두 필드로 나뉘어 있어, 지연
 * 계산({@code now - publishedAt})을 하려면 하나의 long으로 합쳐야 한다.</p>
 */
public final class EpochNanos {

    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    private EpochNanos() {
    }

    /** 현재 시각을 epoch 나노초로 반환한다. */
    public static long now() {
        return of(Instant.now());
    }

    /** 주어진 {@link Instant}을 epoch 나노초로 변환한다. */
    public static long of(Instant instant) {
        return instant.getEpochSecond() * NANOS_PER_SECOND + instant.getNano();
    }
}
