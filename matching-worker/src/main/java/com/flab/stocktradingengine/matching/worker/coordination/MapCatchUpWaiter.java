package com.flab.stocktradingengine.matching.worker.coordination;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.function.LongSupplier;

/**
 * {@link MapSync}의 구현. 맵 토픽의 지금 끝 위치(end offset)를 알아낸 뒤, 맵 읽기가 읽은 위치가 거기에 닿을 때까지 기다린다. 끝 위치를
 * 알아내지 못하면(브로커 응답 없음 등) 닿았다고 보지 않는다.
 */
public final class MapCatchUpWaiter implements MapSync {

    private static final Logger log = System.getLogger(MapCatchUpWaiter.class.getName());

    private final LongSupplier endOffset;
    private final LongSupplier consumedOffset;
    private final long timeoutMillis;
    private final java.util.concurrent.atomic.AtomicInteger consecutiveFailures = new java.util.concurrent.atomic.AtomicInteger();
    private volatile int failureLimit;
    private volatile Runnable failureAction = () -> { };

    /**
     * @param endOffset 맵 토픽의 지금 끝 위치를 알아내는 함수(브로커에 묻는다)
     * @param consumedOffset 맵 읽기가 읽어 반영한 다음 위치
     */
    public MapCatchUpWaiter(LongSupplier endOffset, LongSupplier consumedOffset, long timeoutMillis) {
        this.endOffset = endOffset;
        this.consumedOffset = consumedOffset;
        this.timeoutMillis = timeoutMillis;
    }

    /**
     * 따라오지 못한 판정이 연속 이만큼이면 알린다. 성공하면 횟수를 처음부터 센다. 알린 뒤에도 실패가 이어지면 이만큼 실패할 때마다 다시 알린다.
     * 읽기가 조용히 멈춘 경우를 잡는 데 쓴다: 읽기 thread의 poll은 브로커가 끊겨도 500ms마다 빈 결과로 돌아오므로 마지막 poll 시각은 계속
     * 갱신되어 쓸 수 없고, 읽은 위치가 끝 위치에 닿는지가 정지를 드러낸다.
     */
    public void onRepeatedFailure(int limit, Runnable action) {
        this.failureLimit = limit;
        this.failureAction = action;
    }

    @Override
    public boolean awaitCaughtUp() {
        boolean caughtUp = awaitCaughtUpOnce();
        if (caughtUp) {
            consecutiveFailures.set(0);
        } else if (failureLimit > 0 && consecutiveFailures.incrementAndGet() >= failureLimit) {
            consecutiveFailures.set(0);
            failureAction.run();
        }
        return caughtUp;
    }

    private boolean awaitCaughtUpOnce() {
        long end;
        try {
            end = endOffset.getAsLong();
        } catch (RuntimeException e) {
            log.log(Level.ERROR, "[맵] 맵 토픽의 끝 위치를 알아내지 못함: " + e);
            return false;
        }
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        while (consumedOffset.getAsLong() < end) {
            if (System.nanoTime() > deadline) {
                log.log(Level.ERROR, "[맵] 맵 읽기가 " + timeoutMillis + "ms 안에 끝 위치 " + end + "까지 따라오지 못함: 읽은 위치="
                    + consumedOffset.getAsLong());
                return false;
            }
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }
}
