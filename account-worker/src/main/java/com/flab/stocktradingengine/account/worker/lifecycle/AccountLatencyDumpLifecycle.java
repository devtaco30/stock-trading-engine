package com.flab.stocktradingengine.account.worker.lifecycle;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.SmartLifecycle;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flab.stocktradingengine.time.LatencyHistogram;
import com.flab.stocktradingengine.time.LatencySnapshot;

/**
 * 끝점①(접수·예약) 접수 지연 측정(decision_records/v1-v2-e2e-measurement.md) 결과를 파일로
 * 내보낸다.
 *
 * <h3>두 경로 — 트리거 파일(주)과 종료 시 안전망(보조)</h3>
 * <p>부하 측정은 앱을 재기동하지 않고 한 프로세스 안에서 워밍업·측정 패스 여러 개를 연달아 돌린다
 * (재기동해도 스냅샷·저널이 계좌 상태를 복구해버려 리셋이 안 되기 때문). 그래서 패스 경계마다
 * "지금까지 쌓인 걸 꺼내고 리셋"할 방법이 필요하다 — {@link #snapshotTriggerPath}에 파일이 생기면
 * 그 파일 내용(한 줄, 출력 경로)을 읽어 그 경로에 스냅샷({@link LatencyHistogram#snapshotAndReset()})을
 * 쓰고 트리거 파일을 지운다(부하 드라이버가 그 삭제를 "처리 완료" 신호로 폴링한다). 트리거를 못
 * 보내고 죽는 경우를 대비해, {@link #stop()}에서도 그 시점까지 안 꺼낸 나머지를 {@link #outputPath}에
 * 한 번 더 쓴다(안전망) — 트리거 경로가 비어 있으면(설정 안 함) 이 안전망 하나만 동작한다.</p>
 */
public class AccountLatencyDumpLifecycle implements SmartLifecycle {

    private static final Logger log = System.getLogger(AccountLatencyDumpLifecycle.class.getName());

    // AccountEngineLifecycle(기본 phase 0)이 소비자 스레드를 완전히 멈춘 뒤에야 종료 시 안전망
    // 덤프가 그 뒤테일 기록까지 담을 수 있다 — 더 낮은 phase는 더 늦게 멈춘다(Spring: stop은 phase
    // 내림차순 — 큰 phase부터 먼저 멈춘다).
    private static final int PHASE = -1;
    private static final long POLL_INTERVAL_MILLIS = 200L;
    private static final long POLL_JOIN_TIMEOUT_MILLIS = 1000L;

    private final LatencyHistogram histogram;
    private final String outputPath;
    private final String snapshotTriggerPath;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicBoolean running = new AtomicBoolean(false);

    private Thread pollThread;

    public AccountLatencyDumpLifecycle(LatencyHistogram histogram, String outputPath, String snapshotTriggerPath) {
        this.histogram = histogram;
        this.outputPath = outputPath;
        this.snapshotTriggerPath = snapshotTriggerPath;
    }

    @Override
    public void start() {
        running.set(true);
        if (snapshotTriggerPath != null && !snapshotTriggerPath.isBlank()) {
            pollThread = new Thread(this::pollLoop, "account-latency-trigger-poll");
            pollThread.setDaemon(true);
            pollThread.start();
        }
    }

    private void pollLoop() {
        Path triggerPath = Path.of(snapshotTriggerPath);
        while (running.get()) {
            if (Files.exists(triggerPath)) {
                handleTrigger(triggerPath);
            }
            try {
                Thread.sleep(POLL_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /** 트리거 파일 내용(한 줄=출력 경로)을 읽어 그 경로에 스냅샷을 쓰고, 트리거 파일을 지운다. */
    private void handleTrigger(Path triggerPath) {
        try {
            List<String> lines = Files.readAllLines(triggerPath, StandardCharsets.UTF_8);
            if (lines.isEmpty() || lines.get(0).isBlank()) {
                return; // 부하 드라이버가 아직 다 쓰는 중일 수 있다 — 다음 폴에서 다시 본다.
            }
            String destinationPath = lines.get(0).trim();
            writeSnapshot(destinationPath, histogram.snapshotAndReset());
            Files.deleteIfExists(triggerPath);
        } catch (IOException e) {
            // 계측 부가 기능 — 실패해도 앱 본체(주문 처리)를 죽이면 안 된다. 트리거 파일이 안
            // 지워지므로 부하 드라이버 쪽에서도 타임아웃으로 알아챈다.
            log.log(Level.WARNING, "[계좌 지연 측정] 트리거 처리 실패: trigger=" + triggerPath, e);
        }
    }

    private void writeSnapshot(String destinationPath, LatencySnapshot snapshot) throws IOException {
        Path path = Path.of(destinationPath);
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        objectMapper.writeValue(path.toFile(), snapshot);
    }

    @Override
    public void stop() {
        running.set(false);
        if (pollThread != null) {
            try {
                pollThread.join(POLL_JOIN_TIMEOUT_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            writeSnapshot(outputPath, histogram.snapshotAndReset());
        } catch (IOException e) {
            log.log(Level.WARNING, "[계좌 지연 측정] 종료 시 안전망 덤프 실패: output=" + outputPath, e);
        }
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
