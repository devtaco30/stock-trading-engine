package com.flab.stocktradingengine.matching.worker.recovery;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.concurrent.atomic.AtomicBoolean;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;

import com.flab.stocktradingengine.matching.disruptor.io.MatchingSnapshotSink;

/**
 * 러닝 중 스냅샷을 디스크에 쓰는 전용 스레드(I6 U2) — {@link MatchingSnapshotSink} 구현체.
 * account-worker {@code AccountSnapshotWriter}와 같은 SPSC outbox 큐 + 전용 스레드 패턴이다.
 *
 * <h3>왜 소비자(매칭 엔진) 스레드가 직접 안 쓰나</h3>
 * <p>파일 I/O(특히 {@link MatchingSnapshotStore#write(long, byte[])}의 fsync)는 디스크 상태에
 * 따라 수 ms~수십 ms까지 걸릴 수 있다. single-writer 소비자 스레드가 그 시간만큼 블록되면 그동안
 * 들어온 주문이 전부 지연된다(ADR-024) — 그래서 소비자는 바이트 직렬화까지만 하고 큐에 넣기만
 * 한다({@link #offer}, 논블로킹), 실제 쓰기는 이 클래스의 전용 스레드가 한다.</p>
 *
 * <h3>durableSeq</h3>
 * <p>{@link #durableSeq()}는 fsync까지 끝난 가장 최근 스냅샷의 appliedSeq만 보고한다 — 매칭은
 * 이 값으로 가지치기를 하지 않는다(I6 D3, 범위 밖) — 나중에 매칭 쪽 디스크 회수(저널 recording
 * 정리)의 경계로만 쓰인다.</p>
 */
public class MatchingSnapshotWriter implements MatchingSnapshotSink, AutoCloseable {

    private static final Logger log = System.getLogger(MatchingSnapshotWriter.class.getName());

    // 스냅샷은 N건(기본 10_000)마다 한 번뿐이라 큐가 깊을 필요가 없다 — 쓰기가 여러 주기만큼
    // 밀려도(그 자체가 이상 신호) 담아둘 여유만 있으면 된다.
    private static final int QUEUE_CAPACITY = 4;
    private static final long CLOSE_JOIN_TIMEOUT_MILLIS = 1000L;

    private final MatchingSnapshotStore snapshotStore;
    private final long journalRecordingId;
    private final OneToOneConcurrentArrayQueue<SnapshotWriteTask> queue = new OneToOneConcurrentArrayQueue<>(QUEUE_CAPACITY);
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile long durableSeq = 0L;
    private Thread writerThread;

    public MatchingSnapshotWriter(MatchingSnapshotStore snapshotStore, long journalRecordingId) {
        this.snapshotStore = snapshotStore;
        this.journalRecordingId = journalRecordingId;
    }

    /** 매칭 엔진 소비자 스레드가 호출한다 — 큐에 넣기만 하고 즉시 돌아간다(논블로킹). */
    @Override
    public boolean offer(byte[] snapshotBytes, long appliedSeq) {
        return queue.offer(new SnapshotWriteTask(snapshotBytes, appliedSeq));
    }

    @Override
    public long durableSeq() {
        return durableSeq;
    }

    /** 쓰기 스레드를 기동한다. */
    public void start() {
        running.set(true);
        writerThread = new Thread(this::writeLoop, "matching-snapshot-writer");
        writerThread.setDaemon(true);
        writerThread.start();
    }

    private void writeLoop() {
        while (running.get()) {
            SnapshotWriteTask task = queue.poll();
            if (task != null) {
                writeTask(task);
            }
            idleStrategy.idle(task != null ? 1 : 0);
        }
    }

    private void writeTask(SnapshotWriteTask task) {
        try {
            snapshotStore.write(journalRecordingId, task.snapshotBytes());
            durableSeq = task.appliedSeq(); // fsync까지 끝난 뒤에만 durable로 보고한다
        } catch (RuntimeException e) {
            // 이번 회차 쓰기가 실패해도 프로세스는 계속 돈다 — 다음 스냅샷 주기가 다시 시도한다.
            log.log(Level.ERROR, "[매칭] 러닝 중 스냅샷 쓰기 실패(appliedSeq=" + task.appliedSeq() + ")", e);
        }
    }

    /** 쓰기 스레드를 멈추고 종료를 기다린다. 큐에 남은 항목은 드레인하지 않는다(best-effort). */
    @Override
    public void close() {
        running.set(false);
        if (writerThread != null) {
            try {
                writerThread.join(CLOSE_JOIN_TIMEOUT_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private record SnapshotWriteTask(byte[] snapshotBytes, long appliedSeq) {
    }
}
