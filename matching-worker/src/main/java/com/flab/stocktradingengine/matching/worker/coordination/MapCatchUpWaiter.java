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

    /**
     * @param endOffset 맵 토픽의 지금 끝 위치를 알아내는 함수(브로커에 묻는다)
     * @param consumedOffset 맵 읽기가 읽어 반영한 다음 위치
     */
    public MapCatchUpWaiter(LongSupplier endOffset, LongSupplier consumedOffset, long timeoutMillis) {
        this.endOffset = endOffset;
        this.consumedOffset = consumedOffset;
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public boolean awaitCaughtUp() {
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
