package com.flab.stocktradingengine.time;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class LatencyHistogramTest {

    @Test
    @DisplayName("꺼져 있으면 record해도 스냅샷이 비어 있다")
    void 꺼져있으면_기록_안한다() {
        LatencyHistogram histogram = new LatencyHistogram(false);

        histogram.record(EpochNanos.now() - 10_000_000L);

        assertEquals(LatencySnapshot.empty(), histogram.snapshotAndReset());
    }

    @Test
    @DisplayName("켜져 있어도 아무것도 기록하지 않으면 스냅샷이 비어 있다")
    void 기록없으면_빈_스냅샷() {
        LatencyHistogram histogram = new LatencyHistogram(true);

        assertEquals(LatencySnapshot.empty(), histogram.snapshotAndReset());
    }

    @Test
    @DisplayName("켜져 있으면 경과 시간을 기록해 백분위로 조회할 수 있다")
    void 켜져있으면_경과시간을_백분위로_조회() {
        LatencyHistogram histogram = new LatencyHistogram(true);
        long elapsedNanos = TimeUnit.MILLISECONDS.toNanos(20);

        for (int i = 0; i < 100; i++) {
            histogram.record(EpochNanos.now() - elapsedNanos);
        }

        LatencySnapshot snapshot = histogram.snapshotAndReset();
        assertEquals(100, snapshot.count());
        assertTrue(snapshot.p50Nanos() >= elapsedNanos, "p50이 기록한 경과시간 이상이어야 한다");
        assertTrue(snapshot.p50Nanos() < elapsedNanos + TimeUnit.MILLISECONDS.toNanos(5),
            "p50이 기록값 근처여야 한다(측정 사이 지터 허용)");
        assertTrue(snapshot.maxNanos() >= snapshot.p99Nanos(), "max는 p99 이상이어야 한다");
    }

    @Test
    @DisplayName("스냅샷을 꺼내면 리셋돼, 그다음 스냅샷은 그 뒤에 새로 기록한 것만 담는다(부하 측정 패스 경계용)")
    void 스냅샷_후_리셋된다() {
        LatencyHistogram histogram = new LatencyHistogram(true);
        histogram.record(EpochNanos.now() - TimeUnit.MILLISECONDS.toNanos(10));
        histogram.record(EpochNanos.now() - TimeUnit.MILLISECONDS.toNanos(10));

        LatencySnapshot first = histogram.snapshotAndReset();
        assertEquals(2, first.count());

        LatencySnapshot secondEmpty = histogram.snapshotAndReset();
        assertEquals(LatencySnapshot.empty(), secondEmpty, "리셋 직후엔 새로 기록한 게 없으니 비어야 한다");

        histogram.record(EpochNanos.now() - TimeUnit.MILLISECONDS.toNanos(10));
        LatencySnapshot third = histogram.snapshotAndReset();
        assertEquals(1, third.count(), "리셋 뒤 새로 기록한 1건만 담겨야 한다");
    }

    @Test
    @DisplayName("극단적으로 큰 경과시간(오토리사이징 한계 근처)을 기록해도 예외 없이 처리된다 (2b 리뷰 — 계측이 핫패스를 죽이면 안 됨)")
    void 극단값을_기록해도_예외가_안_난다() {
        LatencyHistogram histogram = new LatencyHistogram(true);

        // publishedAtEpochNanos=1이면 경과시간이 EpochNanos.now() 자체(약 55년 상당) — 오토리사이징
        // 히스토그램이 감당해야 하는 거의 최대 범위. 예외 없이 끝나야 한다(1-2건 흘러도 처리는 계속).
        assertDoesNotThrow(() -> histogram.record(1L));

        LatencySnapshot snapshot = histogram.snapshotAndReset();
        assertEquals(1, snapshot.count(), "예외 없이 처리됐으면 정상 기록도 1건 남아야 한다");
    }

    @Test
    @DisplayName("여러 스레드가 동시에 기록해도 안전하다")
    void 여러_스레드_기록이_안전하다() throws InterruptedException {
        LatencyHistogram histogram = new LatencyHistogram(true);
        int threadCount = 8;
        int recordsPerThread = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch done = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            pool.submit(() -> {
                try {
                    for (int r = 0; r < recordsPerThread; r++) {
                        histogram.record(EpochNanos.now() - TimeUnit.MILLISECONDS.toNanos(5));
                    }
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(done.await(5, TimeUnit.SECONDS), "5초 안에 모든 스레드가 기록을 끝내야 한다");
        pool.shutdown();

        LatencySnapshot snapshot = histogram.snapshotAndReset();
        assertEquals((long) threadCount * recordsPerThread, snapshot.count());
    }
}
