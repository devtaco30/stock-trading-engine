package com.flab.stocktradingengine.account.worker.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flab.stocktradingengine.time.EpochNanos;
import com.flab.stocktradingengine.time.LatencyHistogram;

/**
 * 끝점① 접수 지연 측정(decision_records/v1-v2-e2e-measurement.md) 파일 출력 — 트리거 파일로
 * 부하 측정 패스 경계마다 스냅샷+리셋, 종료 시 안전망 덤프를 검증한다.
 */
class AccountLatencyDumpLifecycleTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private AccountLatencyDumpLifecycle lifecycle;

    @AfterEach
    void tearDown() {
        if (lifecycle != null && lifecycle.isRunning()) {
            lifecycle.stop();
        }
    }

    @Test
    @DisplayName("트리거 경로가 없으면 종료 시 안전망 덤프만 쓴다")
    void 트리거_없이_종료시_덤프만_쓴다(@TempDir Path tempDir) throws IOException {
        LatencyHistogram histogram = new LatencyHistogram(true);
        histogram.record(EpochNanos.now() - TimeUnit.MILLISECONDS.toNanos(10));
        Path output = tempDir.resolve("latency-v2.json");
        lifecycle = new AccountLatencyDumpLifecycle(histogram, output.toString(), "");

        lifecycle.start();
        lifecycle.stop();

        Map<?, ?> written = objectMapper.readValue(output.toFile(), Map.class);
        assertThat(written.get("count")).isEqualTo(1);
    }

    @Test
    @DisplayName("트리거 파일이 생기면 그 내용(출력 경로)에 스냅샷을 쓰고 트리거 파일을 지운다")
    void 트리거가_오면_스냅샷을_쓰고_트리거를_지운다(@TempDir Path tempDir) throws Exception {
        LatencyHistogram histogram = new LatencyHistogram(true);
        histogram.record(EpochNanos.now() - TimeUnit.MILLISECONDS.toNanos(10));
        histogram.record(EpochNanos.now() - TimeUnit.MILLISECONDS.toNanos(20));
        Path finalOutput = tempDir.resolve("latency-v2-final.json");
        Path triggerPath = tempDir.resolve("latency-trigger-v2");
        Path pass1Output = tempDir.resolve("latency-v2-pass1.json");
        lifecycle = new AccountLatencyDumpLifecycle(histogram, finalOutput.toString(), triggerPath.toString());

        lifecycle.start();
        Files.writeString(triggerPath, pass1Output.toString(), StandardCharsets.UTF_8);

        awaitDeleted(triggerPath);

        Map<?, ?> written = objectMapper.readValue(pass1Output.toFile(), Map.class);
        assertThat(written.get("count")).isEqualTo(2);
    }

    @Test
    @DisplayName("트리거 처리 뒤엔 히스토그램이 리셋돼, 다음 트리거는 그 뒤에 기록한 것만 담는다")
    void 트리거_처리_뒤_리셋된다(@TempDir Path tempDir) throws Exception {
        LatencyHistogram histogram = new LatencyHistogram(true);
        histogram.record(EpochNanos.now() - TimeUnit.MILLISECONDS.toNanos(10));
        Path finalOutput = tempDir.resolve("latency-v2-final.json");
        Path triggerPath = tempDir.resolve("latency-trigger-v2");
        Path pass1Output = tempDir.resolve("latency-v2-pass1.json");
        Path pass2Output = tempDir.resolve("latency-v2-pass2.json");
        lifecycle = new AccountLatencyDumpLifecycle(histogram, finalOutput.toString(), triggerPath.toString());
        lifecycle.start();

        Files.writeString(triggerPath, pass1Output.toString(), StandardCharsets.UTF_8);
        awaitDeleted(triggerPath);

        histogram.record(EpochNanos.now() - TimeUnit.MILLISECONDS.toNanos(5));
        Files.writeString(triggerPath, pass2Output.toString(), StandardCharsets.UTF_8);
        awaitDeleted(triggerPath);

        Map<?, ?> pass1 = objectMapper.readValue(pass1Output.toFile(), Map.class);
        Map<?, ?> pass2 = objectMapper.readValue(pass2Output.toFile(), Map.class);
        assertThat(pass1.get("count")).isEqualTo(1);
        assertThat(pass2.get("count")).isEqualTo(1);
    }

    private void awaitDeleted(Path triggerPath) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (Files.exists(triggerPath)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("3초 안에 트리거 파일이 처리(삭제)되지 않음: " + triggerPath);
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("대기 중 인터럽트됨", e);
            }
        }
    }
}
