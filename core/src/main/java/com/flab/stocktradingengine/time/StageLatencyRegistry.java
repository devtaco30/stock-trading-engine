package com.flab.stocktradingengine.time;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.HdrHistogram.Histogram;

/**
 * 구간 분해 측정의 단계별 지연 히스토그램을 이름으로 모아 두는 등록부(lld-C-segments.md 4-3절).
 *
 * <p>프로세스마다 하나를 두고, 단계(예: "A", "B", "journal_append")마다 {@link LatencyHistogram}을 하나씩
 * 돌려준다. 호출하는 코드는 단계 시각의 차이를 {@link LatencyHistogram#recordNanos}로 기록하고, 하네스가 트리거를
 * 줄 때 {@link #dumpTo}가 모든 단계를 JSON 하나로 내보낸다.</p>
 *
 * <h3>꺼져 있으면 비용이 분기 하나</h3>
 * <p>{@code enabled=false}면 {@link #stage}가 모든 이름에 같은 꺼진 히스토그램 하나를 돌려준다.
 * 이름마다 객체를 만들지 않고, 기록은 {@link LatencyHistogram}의 분기 하나로 끝난다.</p>
 *
 * <h3>초 단위 시계열과 라운드 스냅샷이 서로의 기록을 지우지 않는다</h3>
 * <p>시계열은 1초마다 구간 히스토그램을 꺼내므로 그만큼 라운드 스냅샷에서 사라진다. 그래서 꺼낼 때마다
 * 단계별 누적 히스토그램에 합친다. 라운드 스냅샷은 누적분과 아직 안 꺼낸 구간을 합해 만들고 누적분을
 * 비운다. 어느 경로로 꺼내든 기록은 한 번만 세어진다.</p>
 */
public final class StageLatencyRegistry {

    private static final Pattern VALID_NAME = Pattern.compile("[A-Za-z0-9_.@-]+");
    private static final LatencyHistogram DISABLED_STAGE = new LatencyHistogram(false);
    private static final String SERIES_HEADER = "epochSecond,count,p50Micros,p99Micros";
    private static final int SIGNIFICANT_VALUE_DIGITS = 3;

    private final boolean enabled;
    private final ConcurrentHashMap<String, Stage> stages = new ConcurrentHashMap<>();
    private final Object drainLock = new Object();

    public StageLatencyRegistry(boolean enabled) {
        this.enabled = enabled;
    }

    /** 계측이 켜져 있는지. 지점 코드가 시각을 찍기 전에 분기 한 번으로 건너뛰는 데 쓴다. */
    public boolean isEnabled() {
        return enabled;
    }

    /** 단계 이름에 해당하는 히스토그램을 돌려준다(없으면 만든다). 이름은 영문·숫자와 {@code _ . @ -}만 허용한다. */
    public LatencyHistogram stage(String name) {
        validateName(name);
        if (!enabled) {
            return DISABLED_STAGE;
        }
        Stage stage = stages.computeIfAbsent(name, key -> new Stage());
        return stage.histogram;
    }

    /**
     * 모든 단계의 스냅샷을 이름 순으로 꺼내고 내부를 비운다(라운드 경계용). 꺼져 있으면 빈 맵. 기록이
     * 없는 단계도 count 0인 스냅샷으로 나온다.
     */
    public Map<String, StageSnapshot> snapshotAndResetAll() {
        Map<String, StageSnapshot> snapshots = new TreeMap<>();
        if (!enabled) {
            return snapshots;
        }
        synchronized (drainLock) {
            for (Map.Entry<String, Stage> entry : stages.entrySet()) {
                Stage stage = entry.getValue();
                drainIntoAccumulated(stage);
                StageSnapshot snapshot = toSnapshot(stage.accumulated);
                stage.accumulated.reset();
                snapshots.put(entry.getKey(), snapshot);
            }
        }
        return snapshots;
    }

    /** {@link #snapshotAndResetAll}의 결과를 JSON 하나로 임시 파일에 쓴 뒤 원자적으로 옮긴다. */
    public void dumpTo(Path destination) throws IOException {
        Map<String, StageSnapshot> snapshots = snapshotAndResetAll();
        String json = toJson(snapshots);
        Path temporary = destination.resolveSibling(destination.getFileName() + ".tmp");
        Files.writeString(temporary, json + "\n", StandardCharsets.UTF_8);
        Files.move(temporary, destination, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    /** 단계 하나의 초 단위 시계열을 CSV 파일에 쓰도록 지정한다. 첫 {@link #tickSeries}에서 파일을 새로 만든다. */
    public void enableSeries(String stageName, Path csvFile) {
        validateName(stageName);
        if (!enabled) {
            return;
        }
        Stage stage = stages.computeIfAbsent(stageName, key -> new Stage());
        stage.seriesFile = csvFile;
    }

    /**
     * 시계열이 지정된 단계마다 마지막 tick 이후의 구간을 꺼내 {@code epochSecond,count,p50Micros,p99Micros}
     * 한 줄을 덧붙인다. 기록이 없는 초도 count 0으로 적는다(시각 순서가 끊기지 않게). 1초마다 한 스레드가 부른다.
     */
    public void tickSeries(long epochSecond) throws IOException {
        if (!enabled) {
            return;
        }
        synchronized (drainLock) {
            for (Stage stage : stages.values()) {
                if (stage.seriesFile != null) {
                    writeSeriesLine(stage, epochSecond);
                }
            }
        }
    }

    /** 스냅샷 맵을 단계 이름 순 JSON 객체 한 줄로 만든다. */
    public static String toJson(Map<String, StageSnapshot> snapshots) {
        StringBuilder json = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, StageSnapshot> entry : new TreeMap<>(snapshots).entrySet()) {
            if (!first) {
                json.append(',');
            }
            first = false;
            StageSnapshot snapshot = entry.getValue();
            json.append('"').append(entry.getKey()).append("\":{")
                .append("\"count\":").append(snapshot.count())
                .append(",\"p50Micros\":").append(snapshot.p50Micros())
                .append(",\"p95Micros\":").append(snapshot.p95Micros())
                .append(",\"p99Micros\":").append(snapshot.p99Micros())
                .append(",\"maxMicros\":").append(snapshot.maxMicros())
                .append(",\"meanMicros\":").append(String.format(Locale.ROOT, "%.3f", snapshot.meanMicros()))
                .append('}');
        }
        json.append('}');
        return json.toString();
    }

    private void writeSeriesLine(Stage stage, long epochSecond) throws IOException {
        Histogram interval = drainIntoAccumulated(stage);
        StageSnapshot snapshot = toSnapshot(interval);
        if (!stage.seriesHeaderWritten) {
            Files.writeString(stage.seriesFile, SERIES_HEADER + "\n", StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
            stage.seriesHeaderWritten = true;
        }
        String line = epochSecond + "," + snapshot.count() + "," + snapshot.p50Micros() + ","
            + snapshot.p99Micros() + "\n";
        Files.writeString(stage.seriesFile, line, StandardCharsets.UTF_8,
            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    /** 기록기에서 구간을 꺼내 누적 히스토그램에 합치고, 방금 꺼낸 구간을 돌려준다. */
    private Histogram drainIntoAccumulated(Stage stage) {
        Histogram interval = stage.histogram.drainInterval();
        stage.accumulated.add(interval);
        return interval;
    }

    private static StageSnapshot toSnapshot(Histogram histogram) {
        long count = histogram.getTotalCount();
        if (count == 0) {
            return StageSnapshot.empty();
        }
        return new StageSnapshot(
            count,
            toMicros(histogram.getValueAtPercentile(50.0)),
            toMicros(histogram.getValueAtPercentile(95.0)),
            toMicros(histogram.getValueAtPercentile(99.0)),
            toMicros(histogram.getMaxValue()),
            histogram.getMean() / 1000.0);
    }

    /** 나노초를 마이크로초로 환산한다. 0.5µs 이상은 올린다. */
    private static long toMicros(long nanos) {
        return (nanos + 500L) / 1000L;
    }

    private static void validateName(String name) {
        if (name == null || !VALID_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("단계 이름은 영문·숫자와 _ . @ - 만 쓸 수 있다: [" + name + "]");
        }
    }

    private static final class Stage {
        private final LatencyHistogram histogram = new LatencyHistogram(true);
        private final Histogram accumulated = new Histogram(SIGNIFICANT_VALUE_DIGITS);
        private volatile Path seriesFile;
        private boolean seriesHeaderWritten;
    }
}
