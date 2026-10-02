package com.flab.stocktradingengine.time;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link StageLatencyRegistry}를 파일로 내보내는 실행기(구간 분해 측정, measure/segments 브랜치 한정).
 * Spring에 의존하지 않아 api·account-worker 등 어느 프로세스에서든 빈의 init/destroy 메서드로 붙인다.
 *
 * <ul>
 *   <li><b>트리거 파일:</b> {@code snapshotTriggerPath}에 파일이 생기면 첫 줄(출력 경로)에 {@link
 *       StageLatencyRegistry#dumpTo}로 덤프(스냅샷 후 비움)하고 트리거를 지운다. 부하 드라이버는 트리거가
 *       사라진 것을 처리 완료 신호로 본다. 지우기가 실패해 같은 트리거가 다시 보여도 같은 경로로는 다시
 *       덤프하지 않는다(이미 비워진 등록부가 방금 쓴 정상 덤프를 덮어쓰지 않게) — 기존
 *       {@code AccountLatencyDumpLifecycle}과 같은 규칙이고, 패스마다 출력 경로가 달라야 한다.</li>
 *   <li><b>시계열:</b> {@code seriesStage}와 {@code seriesPath}가 모두 있으면 1초마다 {@link
 *       StageLatencyRegistry#tickSeries}를 부른다.</li>
 *   <li><b>종료 안전망:</b> {@link #stop()}에서 남은 기록을 {@code outputPath}에 한 번 쓴다.</li>
 * </ul>
 *
 * <p>등록부가 꺼져 있으면 스레드도 파일도 만들지 않는다.</p>
 */
public final class StageDumpRunner {

    private static final Logger log = System.getLogger(StageDumpRunner.class.getName());
    private static final long POLL_INTERVAL_MILLIS = 200L;
    private static final long TICK_INTERVAL_MILLIS = 1000L;
    private static final long JOIN_TIMEOUT_MILLIS = 1000L;

    private final StageLatencyRegistry registry;
    private final String outputPath;
    private final String snapshotTriggerPath;
    private final String seriesStage;
    private final String seriesPath;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private Thread pollThread;
    private Thread tickThread;
    // 폴링 스레드만 읽고 쓴다.
    private String lastHandledDestinationPath;

    public StageDumpRunner(StageLatencyRegistry registry, String outputPath, String snapshotTriggerPath,
                           String seriesStage, String seriesPath) {
        this.registry = registry;
        this.outputPath = outputPath;
        this.snapshotTriggerPath = snapshotTriggerPath;
        this.seriesStage = seriesStage;
        this.seriesPath = seriesPath;
    }

    public void start() {
        if (!registry.isEnabled()) {
            return;
        }
        running.set(true);
        if (isSet(snapshotTriggerPath)) {
            pollThread = daemon(this::pollLoop, "stage-dump-trigger-poll");
        }
        if (isSet(seriesStage) && isSet(seriesPath)) {
            registry.enableSeries(seriesStage, Path.of(seriesPath));
            tickThread = daemon(this::tickLoop, "stage-dump-series-tick");
        }
    }

    public void stop() {
        if (!running.getAndSet(false)) {
            return;
        }
        join(pollThread);
        join(tickThread);
        if (isSet(outputPath)) {
            try {
                writeDump(Path.of(outputPath));
            } catch (IOException e) {
                log.log(Level.WARNING, "[구간 지연 측정] 종료 시 안전망 덤프 실패: output=" + outputPath, e);
            }
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    private static boolean isSet(String value) {
        return value != null && !value.isBlank();
    }

    private static Thread daemon(Runnable body, String name) {
        Thread thread = new Thread(body, name);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static void join(Thread thread) {
        if (thread == null) {
            return;
        }
        thread.interrupt();
        try {
            thread.join(JOIN_TIMEOUT_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void pollLoop() {
        Path trigger = Path.of(snapshotTriggerPath);
        while (running.get()) {
            try {
                if (Files.exists(trigger)) {
                    handleTrigger(trigger);
                }
            } catch (RuntimeException e) {
                log.log(Level.WARNING, "[구간 지연 측정] 트리거 폴링 중 예상 못한 예외", e);
            }
            try {
                Thread.sleep(POLL_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private void handleTrigger(Path trigger) {
        try {
            List<String> lines = Files.readAllLines(trigger, StandardCharsets.UTF_8);
            if (lines.isEmpty() || lines.get(0).isBlank()) {
                return;
            }
            String destination = lines.get(0).trim();
            if (!destination.equals(lastHandledDestinationPath)) {
                writeDump(Path.of(destination));
                lastHandledDestinationPath = destination;
            }
            Files.deleteIfExists(trigger);
        } catch (IOException e) {
            log.log(Level.WARNING, "[구간 지연 측정] 트리거 처리 실패: trigger=" + trigger, e);
        }
    }

    private void writeDump(Path destination) throws IOException {
        if (destination.getParent() != null) {
            Files.createDirectories(destination.getParent());
        }
        registry.dumpTo(destination);
    }

    private void tickLoop() {
        while (running.get()) {
            try {
                Thread.sleep(TICK_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                return;
            }
            try {
                registry.tickSeries(System.currentTimeMillis() / 1000L);
            } catch (IOException | RuntimeException e) {
                log.log(Level.WARNING, "[구간 지연 측정] 시계열 기록 실패", e);
            }
        }
    }
}
