package com.flab.stocktradingengine.matching.worker.metrics;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.journal.AeronArchiveMatchingJournal;
import com.flab.stocktradingengine.matching.disruptor.journal.Journal;

import lombok.extern.slf4j.Slf4j;

/**
 * 매칭 엔진의 ring buffer, 저널 offer, 체결 발신 queue 계측 값(12-3절 2번 표의 7·8·9단계)을 1초마다 로그 한 줄로 내보낸다.
 * 계좌 쪽 {@code account-engine} 줄과 같은 모양이며(부하 하네스용 로그), 값은 카운터를 읽기만 하고 출력은 이 전용 데몬 스레드가 한다.
 */
@Slf4j
public class MatchingEngineMetricsReporter implements AutoCloseable {

    private static final long REPORT_INTERVAL_MILLIS = 1000L;
    private static final long CLOSE_JOIN_TIMEOUT_MILLIS = 1000L;

    private final MatchingEngine engine;
    private final LongSupplier fillQueueDepth;
    private final LongSupplier fillEnqueueWaits;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread reporterThread;

    /**
     * @param fillQueueDepth 체결 발신 queue들에 지금 쌓인 체결 수의 합
     * @param fillEnqueueWaits 체결 발신 queue가 가득 차 enqueue가 기다린 횟수의 합
     */
    public MatchingEngineMetricsReporter(MatchingEngine engine, LongSupplier fillQueueDepth, LongSupplier fillEnqueueWaits) {
        this.engine = engine;
        this.fillQueueDepth = fillQueueDepth;
        this.fillEnqueueWaits = fillEnqueueWaits;
    }

    /** 엔진과 (Aeron Archive 저널이면) 저널에서 값을 읽는다. Aeron 저널이 아니면 저널 재시도는 0이다. */
    static MatchingEngineMetrics read(MatchingEngine engine, LongSupplier fillQueueDepth, LongSupplier fillEnqueueWaits) {
        Journal journal = engine.journal();
        long backPressure = 0L;
        long otherRetries = 0L;
        if (journal instanceof AeronArchiveMatchingJournal aeronJournal) {
            backPressure = aeronJournal.offerBackPressureCount();
            otherRetries = aeronJournal.offerOtherRetryCount();
        }
        return new MatchingEngineMetrics(engine.ringBufferRemainingCapacity(), engine.ringFullWaitCount(), backPressure, otherRetries,
            fillQueueDepth.getAsLong(), fillEnqueueWaits.getAsLong());
    }

    /** 누적 값은 구간 증가분으로, 남은 칸과 queue 깊이는 현재 값으로 한 줄을 만든다. */
    static String formatLine(MatchingEngineMetrics previous, MatchingEngineMetrics current) {
        return String.format(
            "matching-engine ringRemaining=%d ringFullWaits=%d journalBackPressure=%d journalOtherRetries=%d fillQueueDepth=%d fillEnqueueWaits=%d",
            current.ringRemaining(),
            current.ringFullWaits() - previous.ringFullWaits(),
            current.journalBackPressure() - previous.journalBackPressure(),
            current.journalOtherRetries() - previous.journalOtherRetries(),
            current.fillQueueDepth(),
            current.fillEnqueueWaits() - previous.fillEnqueueWaits());
    }

    public void start() {
        running.set(true);
        reporterThread = new Thread(this::reportLoop, "matching-engine-metrics-reporter");
        reporterThread.setDaemon(true);
        reporterThread.start();
    }

    private void reportLoop() {
        MatchingEngineMetrics previous = read(engine, fillQueueDepth, fillEnqueueWaits);
        while (running.get()) {
            try {
                Thread.sleep(REPORT_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            MatchingEngineMetrics current = read(engine, fillQueueDepth, fillEnqueueWaits);
            log.info(formatLine(previous, current));
            previous = current;
        }
    }

    @Override
    public void close() {
        running.set(false);
        if (reporterThread != null) {
            reporterThread.interrupt();
            try {
                reporterThread.join(CLOSE_JOIN_TIMEOUT_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
