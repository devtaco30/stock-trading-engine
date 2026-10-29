package com.flab.stocktradingengine.account.worker.messaging;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.extern.slf4j.Slf4j;

/**
 * {@link AeronMatchingOrderSender}의 계측 값을 1초마다 로그 한 줄로 내보낸다.
 * {@code MetricsAccountResultListener}의 리포터와 같은 방식이며(부하 하네스용 로그),
 * 계좌 엔진 스레드는 카운터만 올리고 출력은 이 전용 데몬 스레드가 한다.
 */
@Slf4j
public class MatchingSendMetricsReporter implements AutoCloseable {

    private static final long REPORT_INTERVAL_MILLIS = 1000L;
    private static final long CLOSE_JOIN_TIMEOUT_MILLIS = 1000L;

    private final AeronMatchingOrderSender sender;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread reporterThread;

    public MatchingSendMetricsReporter(AeronMatchingOrderSender sender) {
        this.sender = sender;
    }

    /** 구간 증가분(대기 횟수·대기 시간·재시도)과 현재 쌓인 개수를 한 줄로 만든다. */
    static String formatLine(MatchingSendMetrics previous, MatchingSendMetrics current) {
        long waitCount = current.enqueueWaitCount() - previous.enqueueWaitCount();
        double waitMillis = (current.enqueueWaitNanos() - previous.enqueueWaitNanos()) / 1_000_000.0;
        long retries = current.offerRetryCount() - previous.offerRetryCount();
        return String.format(Locale.ROOT, "matching-send outbox=%d enqueueWaits=%d enqueueWaitMs=%.1f offerRetries=%d",
            current.outboxSize(), waitCount, waitMillis, retries);
    }

    public void start() {
        running.set(true);
        reporterThread = new Thread(this::reportLoop, "matching-send-metrics-reporter");
        reporterThread.setDaemon(true);
        reporterThread.start();
    }

    private void reportLoop() {
        MatchingSendMetrics previous = sender.metrics();
        while (running.get()) {
            try {
                Thread.sleep(REPORT_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            MatchingSendMetrics current = sender.metrics();
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
