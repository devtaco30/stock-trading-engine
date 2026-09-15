package com.flab.stocktradingengine.account.worker.lifecycle;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.SmartLifecycle;

import com.flab.stocktradingengine.account.worker.recovery.AccountSnapshotWriter;

/**
 * {@link AccountSnapshotWriter}의 쓰기 스레드 시작·종료를 Spring 컨텍스트 생명주기에 건다(1-3).
 * {@link AccountStatePublisherLifecycle}과 같은 결 — 큐에 쌓인 항목을 드레인하지 않고 best-effort로
 * 멈춘다(디스크 쓰기가 밀려도 다음 스냅샷 주기가 새로 채운다).
 */
public class AccountSnapshotWriterLifecycle implements SmartLifecycle {

    private static final int PHASE = 1; // AccountEngineLifecycle(기본 phase 0)보다 늦게 시작

    private final AccountSnapshotWriter writer;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public AccountSnapshotWriterLifecycle(AccountSnapshotWriter writer) {
        this.writer = writer;
    }

    @Override
    public void start() {
        writer.start();
        running.set(true);
    }

    @Override
    public void stop() {
        writer.close();
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
