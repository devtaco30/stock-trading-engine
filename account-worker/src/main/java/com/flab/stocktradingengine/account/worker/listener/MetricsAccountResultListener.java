package com.flab.stocktradingengine.account.worker.listener;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import org.springframework.stereotype.Component;

import com.flab.stocktradingengine.account.disruptor.domain.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.domain.RejectReason;

import lombok.extern.slf4j.Slf4j;

/**
 * C7 부하 테스트 하네스 — 콜백 호출 수를 {@link LongAdder} 카운터로 모아 1초마다 로그로 찍는
 * {@link AccountResultListener} 구현체. 핫패스(계좌 엔진 소비자 스레드)에서는 카운터 증가만
 * 한다(락 없는 원자 연산) — 실제 로그 출력은 전용 데몬 스레드가 1초 간격으로 낸다.
 * {@link com.flab.stocktradingengine.account.worker.messaging.AccountStatePublisher}와 같은
 * 이유(엔진 스레드가 I/O를 지면 안 됨)지만, outbox 큐 없이 카운터 스냅샷 차만 찍으면 되므로 더 단순하다.
 */
@Slf4j
@Component
public class MetricsAccountResultListener implements AccountResultListener, AutoCloseable {

    private static final long REPORT_INTERVAL_MILLIS = 1000L;
    private static final long CLOSE_JOIN_TIMEOUT_MILLIS = 1000L;

    private final LongAdder accepted = new LongAdder();
    private final LongAdder sellAccepted = new LongAdder();
    private final LongAdder rejected = new LongAdder();
    private final LongAdder fillApplied = new LongAdder();
    private final LongAdder duplicate = new LongAdder();
    private final AtomicBoolean running = new AtomicBoolean(false);

    private Thread reporterThread;

    @Override
    public void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin) {
        accepted.increment();
    }

    @Override
    public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
        sellAccepted.increment();
    }

    @Override
    public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
        rejected.increment();
    }

    @Override
    public void onFillApplied(long accountId, long orderId, long tradeId, boolean applied) {
        if (applied) {
            fillApplied.increment();
        }
    }

    @Override
    public void onSettlementApplied(long accountId, long settlementRef, boolean applied) {
    }

    @Override
    public void onUnpaidRecorded(long accountId, long tradeId, BigDecimal amount) {
    }

    @Override
    public void onDuplicateRequest(long accountId, String requestId) {
        duplicate.increment();
    }

    /** 현재까지 누적된 카운터 스냅샷. 테스트 검증용이자 리포터 스레드가 델타를 계산하는 재료. */
    public Counts counts() {
        return new Counts(accepted.sum(), sellAccepted.sum(), rejected.sum(), fillApplied.sum(), duplicate.sum());
    }

    /** 리포터 스레드를 기동한다. */
    public void start() {
        running.set(true);
        reporterThread = new Thread(this::reportLoop, "account-metrics-reporter");
        reporterThread.setDaemon(true);
        reporterThread.start();
    }

    private void reportLoop() {
        Counts previous = counts();
        while (running.get()) {
            try {
                Thread.sleep(REPORT_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            Counts current = counts();
            long totalAccepted = current.accepted() + current.sellAccepted();
            long previousTotalAccepted = previous.accepted() + previous.sellAccepted();
            log.info("metrics accepted={} rejected={} fill={} dup={} acceptedTotal={}",
                totalAccepted - previousTotalAccepted,
                current.rejected() - previous.rejected(),
                current.fillApplied() - previous.fillApplied(),
                current.duplicate() - previous.duplicate(),
                totalAccepted);
            previous = current;
        }
    }

    /** 리포터 스레드를 멈추고 종료를 기다린다. */
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

    /** 콜백별 누적 호출 수 스냅샷. */
    public record Counts(long accepted, long sellAccepted, long rejected, long fillApplied, long duplicate) {
    }
}
