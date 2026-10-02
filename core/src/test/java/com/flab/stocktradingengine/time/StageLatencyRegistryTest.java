package com.flab.stocktradingengine.time;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StageLatencyRegistryTest {

    @TempDir
    Path tempDir;

    // ---------- LatencyHistogram.recordNanos ----------

    @Test
    @DisplayName("꺼져 있으면 recordNanos해도 기록되지 않는다")
    void 꺼져있으면_recordNanos는_기록하지_않는다() {
        LatencyHistogram histogram = new LatencyHistogram(false);

        histogram.recordNanos(5_000L);

        LatencySnapshot snapshot = histogram.snapshotAndReset();
        assertEquals(LatencySnapshot.empty(), snapshot);
    }

    @Test
    @DisplayName("켜면 recordNanos가 이미 계산된 경과 시간을 그대로 기록한다")
    void 켜면_recordNanos는_경과시간을_기록한다() {
        LatencyHistogram histogram = new LatencyHistogram(true);

        for (int i = 0; i < 100; i++) {
            histogram.recordNanos(1_000_000L);
        }

        LatencySnapshot snapshot = histogram.snapshotAndReset();
        assertEquals(100, snapshot.count());
        assertTrue(snapshot.p50Nanos() >= 1_000_000L, "p50이 기록값 이상이어야 한다");
        assertTrue(snapshot.p50Nanos() < 1_010_000L, "p50이 기록값 근처여야 한다(유효숫자 3자리 오차 안)");
    }

    @Test
    @DisplayName("음수 경과 시간은 0으로 기록한다(시계 해상도 때문에 같은 지점의 차이가 음수일 수 없지만 방어)")
    void 음수는_0으로_기록한다() {
        LatencyHistogram histogram = new LatencyHistogram(true);

        histogram.recordNanos(-5L);

        LatencySnapshot snapshot = histogram.snapshotAndReset();
        assertEquals(1, snapshot.count());
        assertEquals(0L, snapshot.maxNanos());
    }

    // ---------- 꺼진 레지스트리 ----------

    @Test
    @DisplayName("꺼진 레지스트리는 단계에 기록해도 스냅샷이 비고 덤프는 빈 객체다")
    void 꺼진_레지스트리는_비어_있다() throws IOException {
        StageLatencyRegistry registry = new StageLatencyRegistry(false);
        LatencyHistogram stageA = registry.stage("A");

        stageA.recordNanos(3_000L);

        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();
        assertTrue(snapshots.isEmpty());
        Path destination = tempDir.resolve("off.json");
        registry.dumpTo(destination);
        String json = Files.readString(destination, StandardCharsets.UTF_8).trim();
        assertEquals("{}", json);
    }

    @Test
    @DisplayName("꺼진 레지스트리는 이름이 달라도 같은 꺼진 히스토그램 하나를 돌려줘 이름마다 객체를 만들지 않는다")
    void 꺼진_레지스트리는_객체를_하나만_쓴다() {
        StageLatencyRegistry registry = new StageLatencyRegistry(false);

        LatencyHistogram stageA = registry.stage("A");
        LatencyHistogram stageB = registry.stage("B");

        assertTrue(stageA == stageB);
    }

    // ---------- 켜진 레지스트리 ----------

    @Test
    @DisplayName("단계 이름별로 기록이 분리된다")
    void 단계_이름별로_분리된다() {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        LatencyHistogram stageA = registry.stage("A");
        LatencyHistogram stageB = registry.stage("B");

        for (int i = 0; i < 3; i++) {
            stageA.recordNanos(2_000L);
        }
        for (int i = 0; i < 5; i++) {
            stageB.recordNanos(9_000L);
        }

        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();
        StageSnapshot snapshotA = snapshots.get("A");
        StageSnapshot snapshotB = snapshots.get("B");
        assertEquals(3, snapshotA.count());
        assertEquals(5, snapshotB.count());
        assertEquals(2L, snapshotA.p50Micros());
        assertEquals(9L, snapshotB.p50Micros());
    }

    @Test
    @DisplayName("같은 이름으로 stage를 다시 부르면 같은 히스토그램이다")
    void 같은_이름은_같은_히스토그램() {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);

        LatencyHistogram first = registry.stage("A");
        LatencyHistogram second = registry.stage("A");

        assertTrue(first == second);
    }

    @Test
    @DisplayName("이름에 공백이나 따옴표가 있거나 비어 있으면 거부한다(JSON·CSV를 깨뜨리므로)")
    void 잘못된_이름은_거부한다() {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);

        assertThrows(IllegalArgumentException.class, () -> registry.stage(""));
        assertThrows(IllegalArgumentException.class, () -> registry.stage("a b"));
        assertThrows(IllegalArgumentException.class, () -> registry.stage("a\"b"));
        assertThrows(IllegalArgumentException.class, () -> registry.stage("a,b"));
    }

    @Test
    @DisplayName("스냅샷을 꺼내면 리셋돼 다음 스냅샷은 그 뒤 기록만 담는다")
    void 스냅샷_뒤_리셋된다() {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        LatencyHistogram stageA = registry.stage("A");
        stageA.recordNanos(4_000L);
        stageA.recordNanos(4_000L);

        Map<String, StageSnapshot> first = registry.snapshotAndResetAll();
        stageA.recordNanos(4_000L);
        Map<String, StageSnapshot> second = registry.snapshotAndResetAll();

        StageSnapshot firstA = first.get("A");
        StageSnapshot secondA = second.get("A");
        assertEquals(2, firstA.count());
        assertEquals(1, secondA.count());
    }

    @Test
    @DisplayName("기록이 없는 단계도 count 0과 0 값으로 스냅샷에 나온다(단계가 있다는 사실을 보이려고)")
    void 기록없는_단계도_나온다() {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        registry.stage("C");

        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();

        StageSnapshot snapshotC = snapshots.get("C");
        assertEquals(0, snapshotC.count());
        assertEquals(0L, snapshotC.p99Micros());
    }

    @Test
    @DisplayName("값은 마이크로초로 환산하고 반올림한다(1,499ns는 1µs, 1,500ns는 2µs)")
    void 마이크로초로_반올림한다() {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        LatencyHistogram low = registry.stage("LOW");
        LatencyHistogram high = registry.stage("HIGH");
        low.recordNanos(1_499L);
        high.recordNanos(1_500L);

        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();

        StageSnapshot lowSnapshot = snapshots.get("LOW");
        StageSnapshot highSnapshot = snapshots.get("HIGH");
        assertEquals(1L, lowSnapshot.maxMicros());
        assertEquals(2L, highSnapshot.maxMicros());
    }

    @Test
    @DisplayName("평균은 마이크로초 단위 실수로 나온다")
    void 평균은_마이크로초() {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        LatencyHistogram stageA = registry.stage("A");
        stageA.recordNanos(1_000L);
        stageA.recordNanos(3_000L);

        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();

        double mean = snapshots.get("A").meanMicros();
        assertEquals(2.0, mean, 0.01);
    }

    // ---------- 덤프 JSON ----------

    @Test
    @DisplayName("덤프 JSON은 단계 이름 순으로 count, p50Micros, p95Micros, p99Micros, maxMicros, meanMicros를 가진다")
    void 덤프_JSON_모양() throws IOException {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        LatencyHistogram stageB = registry.stage("B");
        LatencyHistogram stageA = registry.stage("A");
        for (int i = 0; i < 4; i++) {
            stageA.recordNanos(2_000L);
        }
        stageB.recordNanos(7_000L);
        Path destination = tempDir.resolve("stages.json");

        registry.dumpTo(destination);

        String json = Files.readString(destination, StandardCharsets.UTF_8).trim();
        String expectedPrefix = "{\"A\":{\"count\":4,\"p50Micros\":2,\"p95Micros\":2,\"p99Micros\":2,\"maxMicros\":2,\"meanMicros\":2.000},"
            + "\"B\":{\"count\":1,\"p50Micros\":7,\"p95Micros\":7,\"p99Micros\":7,\"maxMicros\":7,\"meanMicros\":7.00";
        // HdrHistogram은 유효숫자 3자리라 평균이 값의 0.1% 안에서 달라질 수 있어(7,000ns → 7.002µs) 마지막 자리는 비교하지 않는다
        assertTrue(json.startsWith(expectedPrefix), json);
        assertTrue(json.endsWith("}}"), json);
    }

    @Test
    @DisplayName("덤프는 임시 파일을 남기지 않고 원자적으로 저장한다")
    void 덤프는_임시파일을_남기지_않는다() throws IOException {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        registry.stage("A").recordNanos(1_000L);
        Path destination = tempDir.resolve("atomic.json");

        registry.dumpTo(destination);

        assertTrue(Files.exists(destination));
        Path temporary = tempDir.resolve("atomic.json.tmp");
        assertFalse(Files.exists(temporary));
    }

    @Test
    @DisplayName("덤프 뒤 기록은 다음 덤프에만 들어간다")
    void 덤프_뒤_리셋() throws IOException {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        LatencyHistogram stageA = registry.stage("A");
        stageA.recordNanos(1_000L);
        Path first = tempDir.resolve("first.json");
        Path second = tempDir.resolve("second.json");

        registry.dumpTo(first);
        stageA.recordNanos(1_000L);
        stageA.recordNanos(1_000L);
        registry.dumpTo(second);

        String secondJson = Files.readString(second, StandardCharsets.UTF_8);
        assertTrue(secondJson.contains("\"count\":2"), secondJson);
    }

    // ---------- 동시 기록 ----------

    @Test
    @DisplayName("스레드 여러 개가 같은 단계에 동시에 기록해도 건수가 맞다")
    void 동시_기록_건수가_맞다() throws InterruptedException {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        LatencyHistogram stageA = registry.stage("A");
        int threads = 8;
        int perThread = 50_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < perThread; i++) {
                    stageA.recordNanos(1_000L + i);
                }
                done.countDown();
            });
        }
        ready.await(10, TimeUnit.SECONDS);
        start.countDown();
        boolean finished = done.await(60, TimeUnit.SECONDS);
        pool.shutdownNow();

        assertTrue(finished);
        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();
        StageSnapshot snapshotA = snapshots.get("A");
        assertEquals((long) threads * perThread, snapshotA.count());
    }

    // ---------- 초 단위 시계열 ----------

    @Test
    @DisplayName("시계열은 머리글 한 줄 뒤에 tick마다 epochSecond,count,p50Micros,p99Micros 한 줄을 적는다")
    void 시계열_파일_모양() throws IOException {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        Path series = tempDir.resolve("series-A.csv");
        registry.enableSeries("A", series);
        LatencyHistogram stageA = registry.stage("A");
        for (int i = 0; i < 3; i++) {
            stageA.recordNanos(5_000L);
        }
        registry.tickSeries(100L);
        stageA.recordNanos(8_000L);
        stageA.recordNanos(8_000L);
        registry.tickSeries(101L);
        registry.tickSeries(102L);

        List<String> lines = Files.readAllLines(series, StandardCharsets.UTF_8);

        assertEquals(List.of(
            "epochSecond,count,p50Micros,p99Micros",
            "100,3,5,5",
            "101,2,8,8",
            "102,0,0,0"), lines);
    }

    @Test
    @DisplayName("시계열이 꺼낸 구간도 라운드 스냅샷에는 그대로 합쳐진다(기록이 사라지지 않음)")
    void 시계열이_라운드_스냅샷을_비우지_않는다() throws IOException {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        registry.enableSeries("A", tempDir.resolve("series-A.csv"));
        LatencyHistogram stageA = registry.stage("A");
        stageA.recordNanos(5_000L);
        stageA.recordNanos(5_000L);
        registry.tickSeries(100L);
        stageA.recordNanos(5_000L);

        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();

        StageSnapshot snapshotA = snapshots.get("A");
        assertEquals(3, snapshotA.count());
    }

    @Test
    @DisplayName("기록하는 동안 tick을 반복해도 시계열 건수 합과 라운드 스냅샷 건수가 모두 전체 기록 건수와 같다")
    void 동시_기록과_tick에서_유실이_없다() throws Exception {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        Path series = tempDir.resolve("series-A.csv");
        registry.enableSeries("A", series);
        LatencyHistogram stageA = registry.stage("A");
        int threads = 4;
        int perThread = 100_000;
        AtomicBoolean stop = new AtomicBoolean(false);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch done = new CountDownLatch(threads);
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                for (int i = 0; i < perThread; i++) {
                    stageA.recordNanos(2_000L);
                }
                done.countDown();
            });
        }
        long second = 0;
        while (done.getCount() > 0) {
            registry.tickSeries(second++);
        }
        registry.tickSeries(second);
        stop.set(true);
        pool.shutdownNow();

        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();
        long seriesTotal = 0;
        List<String> lines = Files.readAllLines(series, StandardCharsets.UTF_8);
        for (int i = 1; i < lines.size(); i++) {
            String[] fields = lines.get(i).split(",");
            seriesTotal += Long.parseLong(fields[1]);
        }
        long expectedTotal = (long) threads * perThread;
        assertEquals(expectedTotal, seriesTotal, "tick이 꺼낸 구간의 합이 전체 기록 건수와 같아야 한다");
        StageSnapshot snapshotA = snapshots.get("A");
        assertEquals(expectedTotal, snapshotA.count(), "라운드 스냅샷에도 같은 기록이 한 번만 들어 있어야 한다");
    }

    @Test
    @DisplayName("시계열을 지정하지 않은 단계에는 tickSeries가 아무 파일도 만들지 않는다")
    void 시계열_미지정이면_파일이_없다() throws IOException {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        registry.stage("A").recordNanos(1_000L);

        registry.tickSeries(100L);

        try (java.util.stream.Stream<Path> files = Files.list(tempDir)) {
            assertEquals(0L, files.count());
        }
    }

    @Test
    void isEnabled는_생성자_인자를_그대로_돌려준다() {
        org.junit.jupiter.api.Assertions.assertTrue(new StageLatencyRegistry(true).isEnabled());
        org.junit.jupiter.api.Assertions.assertFalse(new StageLatencyRegistry(false).isEnabled());
    }
}
