package com.flab.stocktradingengine.account.worker.lifecycle;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.SmartLifecycle;

import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.disruptor.io.AccountFillReceiver;
import com.flab.stocktradingengine.account.worker.recovery.AccountSnapshotStore;

/**
 * graceful stop 시점에 계좌 엔진 스냅샷을 찍어 저장한다(2d-2b, ADR-019 "자체 스냅샷"). matching
 * {@code MatchingSnapshotLifecycle}과 같은 결.
 *
 * <h3>왜 {@link AccountEngineLifecycle}과 분리했는가</h3>
 * <p>엔진 시작·종료 자체와 "종료 후에 스냅샷을 찍는다"는 서로 다른 관심사다. 나눠두면
 * {@code AccountEngineConfigTest}처럼 Aeron Archive 없이 엔진만 가볍게 띄우는 테스트가 이
 * 클래스(AeronArchive 필요)를 몰라도 된다.</p>
 *
 * <h3>순서 — {@link AccountEngineLifecycle}(phase 0)보다 낮은 phase</h3>
 * <p>SmartLifecycle은 낮은 phase부터 시작해 높은 phase부터(역순으로) 멈춘다. 이 빈이 phase -1이면
 * 엔진(phase 0)·수신 스레드(phase 1, 주문·체결 둘 다)가 먼저 멈춘 뒤에야 이 빈이 멈춘다 —
 * {@link AccountEngine#shutdown}(Disruptor drain)이 끝나 소비자 스레드가 quiescent 상태가 된
 * 다음에만 다른 스레드(이 stop() 호출 스레드)가 accounts 를 안전하게 읽을 수 있어서다. 크래시
 * (ungraceful)면 스냅샷을 못 찍고 직전 스냅샷 + 그 뒤 저널 replay로 복구한다(메커니즘 먼저 —
 * 주기적 라이브 스냅샷은 나중 리파인).</p>
 *
 * <h3>fillConsumedPosition (ADR-032, U4a)</h3>
 * <p>{@link AccountFillReceiver}(phase 1)가 이 빈보다 먼저 멈추므로, 그 시점의
 * {@link AccountFillReceiver#consumedPosition()}은 이미 quiescent한 최종값이다 — 체결 스트림
 * (6001)에서 durable하게 반영이 끝난 위치를 그대로 스냅샷에 담아, 재기동 시 그 위치부터 fill을
 * replay하면 된다(U4b).</p>
 *
 * <h3>recordingId — 더 이상 이 클래스가 직접 조회하지 않는다 (1-3)</h3>
 * <p>이전엔 stop() 때마다 카탈로그를 스캔했다. 러닝 중 스냅샷 쓰기({@code AccountSnapshotWriter})도
 * 같은 recordingId가 필요해지면서, {@code AccountJournalArchiveConfig#accountJournalRecordingId}
 * 빈으로 한 번만 조회해 공유한다 — 한 프로세스 실행 동안 이 값은 안 바뀐다.</p>
 *
 * <h3>running을 AtomicBoolean으로 (1-3)</h3>
 * <p>{@link #isRunning()}은 Spring 라이프사이클 관리 스레드가 부를 수 있어 {@link #start}·
 * {@link #stop}을 호출하는 스레드와 다를 수 있다 — plain boolean은 그 가시성을 보장 못 해
 * {@link AtomicBoolean}으로 둔다.</p>
 */
public class AccountSnapshotLifecycle implements SmartLifecycle {

    private static final int PHASE = -1; // AccountEngineLifecycle(phase 0)보다 늦게 멈춘다

    private final AccountEngine engine;
    private final AccountFillReceiver fillReceiver;
    private final AccountSnapshotStore snapshotStore;
    private final long journalRecordingId;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public AccountSnapshotLifecycle(AccountEngine engine, AccountFillReceiver fillReceiver,
            AccountSnapshotStore snapshotStore, Long journalRecordingId) {
        this.engine = engine;
        this.fillReceiver = fillReceiver;
        this.snapshotStore = snapshotStore;
        this.journalRecordingId = journalRecordingId;
    }

    @Override
    public void start() {
        running.set(true);
    }

    @Override
    public void stop() {
        snapshotStore.write(journalRecordingId, fillReceiver.consumedPosition(), engine.snapshot());
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
