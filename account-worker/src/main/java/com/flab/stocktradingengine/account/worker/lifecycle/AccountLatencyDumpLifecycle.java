package com.flab.stocktradingengine.account.worker.lifecycle;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
 *
 * <h3>순서는 "쓰고 → 지운다" 그대로, 다만 삭제 실패가 재처리로 이어지지 않게 한다</h3>
 * <p>부하 드라이버는 트리거 파일이 "사라짐"을 목적지 파일이 다 써졌다는 완료 신호로 폴링한다
 * (dc 스펙) — 그래서 목적지를 다 쓴 뒤에야 트리거를 지워야 하고, 트리거를 먼저 다른 이름으로
 * 옮겨버리면(claim) 목적지가 아직 안 써졌는데도 신호가 먼저 뜬다(실측 — 초기 구현에서 목적지
 * 파일이 빈 상태로 읽히는 걸 테스트가 잡아냄). 그런데 순서를 그대로 두면 삭제가 실패했을 때
 * (디스크 오류 등) 다음 폴이 같은 트리거를 또 보고, 이미 리셋된(거의 빈) 히스토그램으로 방금 쓴
 * 정상 스냅샷을 빈 값으로 덮어써버린다(2b 리뷰 지적). 그래서 {@link #lastHandledDestinationPath}로
 * "이 트리거는 이미 처리했다"를 기억해 뒀다가, 같은 목적지 경로가 다시 보이면 히스토그램 재기록
 * 없이 삭제만 재시도한다 — 삭제가 계속 실패해도 목적지 파일 내용은 절대 다시 안 건드린다.</p>
 *
 * <h3>목적지 파일은 임시 이름에 쓰고 원자적으로 rename한다</h3>
 * <p>드라이버가 트리거 삭제를 보고 바로 목적지를 읽으므로, {@code objectMapper.writeValue}가
 * 파일을 만들고 내용을 채우는 그 짧은 틈을 드라이버가 비어 있는 채로 읽을 수 있다(같은 이유로
 * 테스트가 잡아냄). {@code destinationPath + ".tmp"}에 다 쓴 뒤 {@link Files#move}(ATOMIC_MOVE)로
 * 최종 경로에 옮겨, 목적지 경로는 항상 완성된 내용으로만 나타나게 한다.</p>
 *
 * <h3>드라이버 계약 — 패스마다 목적지 경로가 달라야 한다</h3>
 * <p>{@link #lastHandledDestinationPath}는 "직전에 처리한 목적지 경로와 같은가"만으로 재처리
 * 여부를 판단하고 성공 후에도 비우지 않는다(39 리뷰 비블로킹 노트) — 그래서 서로 다른 패스가
 * 우연히 같은 목적지 경로를 쓰면 두 번째 패스가 조용히 스킵된다. 패스마다 목적지 경로를 다르게
 * 주는 것이 부하 드라이버 쪽 계약이다(워밍업+3패스 테스트가 이미 이렇게 검증돼 있다) — 이 게
 * 지켜지는 한 "직전과 같은 경로"는 항상 "삭제만 실패한 같은 트리거의 재도착"만을 뜻한다.</p>
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
    // 폴링 스레드(pollLoop) 하나만 읽고 쓴다 — 같은 트리거 내용을 다시 보면(삭제 재시도 상황)
    // 히스토그램을 또 건드리지 않기 위한 기억.
    private String lastHandledDestinationPath;

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
            try {
                if (Files.exists(triggerPath)) {
                    handleTrigger(triggerPath);
                }
            } catch (RuntimeException e) {
                // 폴링 스레드 자체가 죽으면 이후 패스 경계를 영영 못 잡는다 — 이번 회차만 걸러내고
                // 계속 돈다(2b 리뷰 지적).
                log.log(Level.WARNING, "[계좌 지연 측정] 트리거 폴링 중 예상 못한 예외", e);
            }
            try {
                Thread.sleep(POLL_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * 트리거 내용(한 줄=목적지 경로)을 읽어 그 경로에 스냅샷을 원자적으로 쓰고, 트리거 파일을
     * 지운다. 직전에 처리한 목적지 경로와 같으면(=삭제가 실패해 같은 트리거가 다시 보인 것)
     * 히스토그램을 다시 건드리지 않고 삭제만 재시도한다.
     */
    private void handleTrigger(Path triggerPath) {
        try {
            List<String> lines = Files.readAllLines(triggerPath, StandardCharsets.UTF_8);
            if (lines.isEmpty() || lines.get(0).isBlank()) {
                return; // 부하 드라이버가 아직 다 쓰는 중일 수 있다 — 다음 폴에서 다시 본다.
            }
            String destinationPath = lines.get(0).trim();
            if (!destinationPath.equals(lastHandledDestinationPath)) {
                writeSnapshot(destinationPath, histogram.snapshotAndReset());
                lastHandledDestinationPath = destinationPath;
            }
            Files.deleteIfExists(triggerPath);
        } catch (IOException e) {
            // 계측 부가 기능 — 실패해도 앱 본체(주문 처리)를 죽이면 안 된다. 트리거 파일이 안
            // 지워지므로 부하 드라이버 쪽에서도 타임아웃으로 알아챈다. writeSnapshot이 이미 성공한
            // 뒤 삭제만 실패한 경우엔 lastHandledDestinationPath가 다음 폴에서 재기록을 막아준다.
            log.log(Level.WARNING, "[계좌 지연 측정] 트리거 처리 실패: trigger=" + triggerPath, e);
        }
    }

    /** {@code destinationPath + ".tmp"}에 다 쓴 뒤 원자적으로 rename한다 — 읽는 쪽이 빈/일부만 쓰인 파일을 못 보게. */
    private void writeSnapshot(String destinationPath, LatencySnapshot snapshot) throws IOException {
        Path path = Path.of(destinationPath);
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
        objectMapper.writeValue(tmp.toFile(), snapshot);
        Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
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
