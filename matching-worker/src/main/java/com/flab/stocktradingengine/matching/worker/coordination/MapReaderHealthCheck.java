package com.flab.stocktradingengine.matching.worker.coordination;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/**
 * 호가창 이동 판정이 없어도 맵 읽기가 정지했는지 보는 주기 점검. 주기마다 맵 토픽의 끝 위치와 읽은 위치를 비교해, 끝에 못 닿았는데
 * 읽은 위치가 앞 점검 때와 같은 채로 연속 {@code stuckLimit}번 이어지면 읽기 재시작을 요청한다. 읽기가 이미 정상이 아니면(재시작 중)
 * 세지 않는다. 끝 위치를 알아내지 못하면(브로커 응답 없음) 정지인지 알 수 없으므로 세지도 버리지도 않는다.
 */
public final class MapReaderHealthCheck implements AutoCloseable {

    private static final Logger log = System.getLogger(MapReaderHealthCheck.class.getName());

    private final LongSupplier endOffset;
    private final LongSupplier consumedOffset;
    private final BooleanSupplier readerHealthy;
    private final int stuckLimit;
    private final Runnable restartAction;
    private long lastConsumed = -1L;
    private int stuckCount;
    private ScheduledExecutorService scheduler;

    /**
     * @param endOffset 맵 토픽의 지금 끝 위치를 알아내는 함수(브로커에 묻는다)
     * @param consumedOffset 맵 읽기가 읽어 반영한 다음 위치
     * @param readerHealthy 맵 읽기가 정상인지
     * @param stuckLimit 읽은 위치가 멈춘 채 이어지는 연속 점검 횟수의 한도
     * @param restartAction 한도에 닿으면 부르는 읽기 재시작 요청
     */
    public MapReaderHealthCheck(
            LongSupplier endOffset, LongSupplier consumedOffset, BooleanSupplier readerHealthy, int stuckLimit, Runnable restartAction) {
        this.endOffset = endOffset;
        this.consumedOffset = consumedOffset;
        this.readerHealthy = readerHealthy;
        this.stuckLimit = stuckLimit;
        this.restartAction = restartAction;
    }

    /** 점검 한 번. 주기 thread 하나에서만 부른다(시험은 직접 부른다). */
    public void checkOnce() {
        if (!readerHealthy.getAsBoolean()) {
            reset();
            return;
        }
        long end;
        try {
            end = endOffset.getAsLong();
        } catch (RuntimeException e) {
            log.log(Level.ERROR, "[맵] 헬스 점검이 맵 토픽의 끝 위치를 알아내지 못함: " + e);
            return;
        }
        long consumed = consumedOffset.getAsLong();
        if (consumed >= end) {
            reset();
            return;
        }
        stuckCount = consumed == lastConsumed ? stuckCount + 1 : 0;
        lastConsumed = consumed;
        if (stuckCount >= stuckLimit) {
            log.log(Level.ERROR, "[맵] 헬스 점검: 읽은 위치 " + consumed + "이(가) 끝 위치 " + end + "에 못 닿은 채 " + stuckLimit
                + "번 연속 멈춰 있음, 읽기 재시작을 요청함");
            reset();
            restartAction.run();
        }
    }

    private void reset() {
        lastConsumed = -1L;
        stuckCount = 0;
    }

    public synchronized void start(long intervalMillis) {
        scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "matching-shard-map-health-check");
            thread.setDaemon(true);
            return thread;
        });
        scheduler.scheduleWithFixedDelay(this::safeCheck, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    // 예외가 한 번 새면 scheduleWithFixedDelay가 이후 실행을 모두 멈춘다.
    private void safeCheck() {
        try {
            checkOnce();
        } catch (RuntimeException e) {
            log.log(Level.ERROR, "[맵] 헬스 점검 중 예외(점검은 계속함): " + e);
        }
    }

    @Override
    public synchronized void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            try {
                scheduler.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            scheduler = null;
        }
    }
}
