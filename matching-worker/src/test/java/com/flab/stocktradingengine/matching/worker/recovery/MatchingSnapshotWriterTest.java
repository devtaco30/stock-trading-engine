package com.flab.stocktradingengine.matching.worker.recovery;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshot;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshotCodec;

/**
 * I6 U2 — 러닝 중 스냅샷 쓰기 스레드가 큐에서 꺼내 실제로 디스크에 쓰고, 확정(durableSeq)을
 * 정확히 보고하며, 한 회차가 실패해도 계속 도는지 검증한다.
 */
class MatchingSnapshotWriterTest {

    private static final MatchingSnapshotCodec CODEC = new MatchingSnapshotCodec();

    @TempDir
    private Path archiveDir;

    private MatchingSnapshotWriter writer;

    @AfterEach
    void tearDown() {
        if (writer != null) {
            writer.close();
        }
    }

    @Test
    void offer한_바이트가_실제로_파일로_나온다() throws InterruptedException {
        MatchingSnapshotStore store = new MatchingSnapshotStore(archiveDir.toFile());
        writer = new MatchingSnapshotWriter(store, 42L);
        writer.start();

        MatchingSnapshot snapshot = new MatchingSnapshot(Map.of(), 777L);
        assertThat(writer.offer(CODEC.encode(snapshot), Map.of(0, 111L), 10_000L)).isTrue();
        awaitDurableSeq(10_000L);

        Optional<StoredMatchingSnapshot> found = store.read();
        assertThat(found).isPresent();
        assertThat(found.get().recordingId()).isEqualTo(42L);
        assertThat(found.get().orderIntakePosition()).isEqualTo(Map.of(0, 111L));
        assertThat(found.get().snapshot()).isEqualTo(snapshot);
    }

    @Test
    void 쓰기_스레드가_돌기_전에는_durableSeq가_오르지_않는다() throws InterruptedException {
        MatchingSnapshotStore store = new MatchingSnapshotStore(archiveDir.toFile());
        writer = new MatchingSnapshotWriter(store, 1L);

        writer.offer(CODEC.encode(new MatchingSnapshot(Map.of(), 0L)), Map.of(), 5_000L);
        assertThat(writer.durableSeq()).isZero();

        writer.start();
        awaitDurableSeq(5_000L);
    }

    @Test
    void 쓰기가_실패해도_다음_회차는_계속_처리된다() throws InterruptedException {
        AtomicInteger callCount = new AtomicInteger(0);
        MatchingSnapshotStore failingOnceStore = new MatchingSnapshotStore(archiveDir.toFile()) {
            @Override
            public void write(long recordingId, Map<Integer, Long> orderIntakePositions, byte[] snapshotBytes) {
                if (callCount.getAndIncrement() == 0) {
                    throw new RuntimeException("의도적 쓰기 실패(테스트)");
                }
                super.write(recordingId, orderIntakePositions, snapshotBytes);
            }
        };
        writer = new MatchingSnapshotWriter(failingOnceStore, 7L);
        writer.start();

        byte[] bytes = CODEC.encode(new MatchingSnapshot(Map.of(), 0L));
        writer.offer(bytes, Map.of(), 1_000L); // 이 회차는 실패
        awaitCallCount(callCount, 1);
        assertThat(writer.durableSeq()).isZero();

        writer.offer(bytes, Map.of(), 2_000L); // 다음 회차는 성공해야 한다 — 쓰기 스레드가 안 죽었다는 증거
        awaitDurableSeq(2_000L);
    }

    private void awaitDurableSeq(long expected) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (writer.durableSeq() != expected) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("2초 안에 durableSeq가 " + expected + "에 도달하지 않음(현재 " + writer.durableSeq() + ")");
            }
            Thread.onSpinWait();
        }
    }

    private void awaitCallCount(AtomicInteger counter, int expected) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (counter.get() < expected) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("2초 안에 write 호출이 " + expected + "번 일어나지 않음");
            }
            Thread.onSpinWait();
        }
    }
}
