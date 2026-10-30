package com.flab.stocktradingengine.account.worker.metrics;

import java.util.concurrent.atomic.AtomicBoolean;

import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.disruptor.journal.AccountJournal;
import com.flab.stocktradingengine.account.disruptor.journal.AeronArchiveAccountJournal;

import lombok.extern.slf4j.Slf4j;

/**
 * 계좌 엔진의 ring buffer와 저널 offer 계측 값을 1초마다 로그 한 줄로 내보낸다.
 * {@code MatchingSendMetricsReporter}와 같은 방식이며(부하 하네스용 로그), 값은 엔진과 저널의 카운터를 읽기만 하고 출력은 이 전용 데몬 스레드가 한다.
 */
@Slf4j
public class AccountEngineMetricsReporter implements AutoCloseable {

    private static final long REPORT_INTERVAL_MILLIS = 1000L;
    private static final long CLOSE_JOIN_TIMEOUT_MILLIS = 1000L;

    private final AccountEngine engine;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread reporterThread;

    public AccountEngineMetricsReporter(AccountEngine engine) {
        this.engine = engine;
    }

    /** 엔진과 (Aeron Archive 저널이면) 저널에서 값을 읽는다. Aeron 저널이 아니면 저널 재시도는 0이다. */
    static AccountEngineMetrics read(AccountEngine engine) {
        AccountJournal journal = engine.journal();
        long backPressure = 0L;
        long otherRetries = 0L;
        if (journal instanceof AeronArchiveAccountJournal aeronJournal) {
            backPressure = aeronJournal.offerBackPressureCount();
            otherRetries = aeronJournal.offerOtherRetryCount();
        }
        return new AccountEngineMetrics(engine.ringBufferRemainingCapacity(), engine.ringFullWaitCount(), engine.ringFullWaitNanos(), backPressure,
            otherRetries);
    }

    /** 누적 값은 구간 증가분으로, 남은 칸은 현재 값으로 한 줄을 만든다. */
    static String formatLine(AccountEngineMetrics previous, AccountEngineMetrics current) {
        return String.format(java.util.Locale.ROOT,
            "account-engine ringRemaining=%d ringFullWaits=%d ringFullWaitMs=%.1f journalBackPressure=%d journalOtherRetries=%d",
            current.ringRemaining(),
            current.ringFullWaits() - previous.ringFullWaits(),
            (current.ringFullWaitNanos() - previous.ringFullWaitNanos()) / 1_000_000.0,
            current.journalBackPressure() - previous.journalBackPressure(),
            current.journalOtherRetries() - previous.journalOtherRetries());
    }

    public void start() {
        running.set(true);
        reporterThread = new Thread(this::reportLoop, "account-engine-metrics-reporter");
        reporterThread.setDaemon(true);
        reporterThread.start();
    }

    private void reportLoop() {
        AccountEngineMetrics previous = read(engine);
        while (running.get()) {
            try {
                Thread.sleep(REPORT_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            AccountEngineMetrics current = read(engine);
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
