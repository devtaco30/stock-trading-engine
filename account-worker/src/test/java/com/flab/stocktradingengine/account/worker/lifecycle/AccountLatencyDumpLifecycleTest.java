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
        awaitWritten(pass1Output);

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
        awaitWritten(pass1Output);

        histogram.record(EpochNanos.now() - TimeUnit.MILLISECONDS.toNanos(5));
        Files.writeString(triggerPath, pass2Output.toString(), StandardCharsets.UTF_8);
        awaitDeleted(triggerPath);
        awaitWritten(pass2Output);

        Map<?, ?> pass1 = objectMapper.readValue(pass1Output.toFile(), Map.class);
        Map<?, ?> pass2 = objectMapper.readValue(pass2Output.toFile(), Map.class);
        assertThat(pass1.get("count")).isEqualTo(1);
        assertThat(pass2.get("count")).isEqualTo(1);
    }

    /**
     * 2b 리뷰 지적 ⑤ — 워밍업 1 + 측정 패스 3, 총 4번 연속 스냅샷+리셋이 돌아도 이전 패스의 표본이
     * 다음 패스로 누적·혼입되지 않는지(패스마다 그 구간에 기록한 것만 담기는지) 직접 확인한다.
     */
    @Test
    @DisplayName("워밍업+측정 3패스, 총 4번 연속 트리거해도 패스마다 그 구간 표본만 담기고 누적되지 않는다")
    void 네번_연속_트리거해도_누적되지_않는다(@TempDir Path tempDir) throws Exception {
        LatencyHistogram histogram = new LatencyHistogram(true);
        Path finalOutput = tempDir.resolve("latency-v2-final.json");
        Path triggerPath = tempDir.resolve("latency-trigger-v2");
        lifecycle = new AccountLatencyDumpLifecycle(histogram, finalOutput.toString(), triggerPath.toString());
        lifecycle.start();

        int[] countsPerPass = {5, 3, 7, 2}; // 워밍업, 패스1, 패스2, 패스3
        for (int pass = 0; pass < countsPerPass.length; pass++) {
            for (int i = 0; i < countsPerPass[pass]; i++) {
                histogram.record(EpochNanos.now() - TimeUnit.MILLISECONDS.toNanos(10));
            }
            Path output = tempDir.resolve("latency-v2-pass" + pass + ".json");
            Files.writeString(triggerPath, output.toString(), StandardCharsets.UTF_8);
            awaitDeleted(triggerPath);
            awaitWritten(output);

            Map<?, ?> written = objectMapper.readValue(output.toFile(), Map.class);
            assertThat(written.get("count")).as("패스 %d 표본 수", pass).isEqualTo(countsPerPass[pass]);
        }
    }

    /**
     * 2b 리뷰 지적 ③ — 트리거 삭제가 실패해(디스크 오류 등) 다음 폴이 같은 내용의 트리거를 다시
     * 보는 상황을 재현한다(코드는 삭제 성공 여부와 무관하게 "같은 목적지 경로를 다시 봤다"만으로
     * 판단하므로, 트리거를 같은 내용으로 다시 써서 그 상황을 그대로 흉내낼 수 있다). 이미 쓴
     * 정상 스냅샷 파일이 두 번째 처리에서 덮어써지지 않아야 한다.
     */
    @Test
    @DisplayName("같은 목적지 경로의 트리거를 다시 봐도(삭제 실패 재현) 이미 쓴 스냅샷을 다시 덮어쓰지 않는다")
    void 같은_트리거_재처리는_스냅샷을_다시_안_쓴다(@TempDir Path tempDir) throws Exception {
        LatencyHistogram histogram = new LatencyHistogram(true);
        Path finalOutput = tempDir.resolve("latency-v2-final.json");
        Path triggerPath = tempDir.resolve("latency-trigger-v2");
        Path pass1Output = tempDir.resolve("latency-v2-pass1.json");
        lifecycle = new AccountLatencyDumpLifecycle(histogram, finalOutput.toString(), triggerPath.toString());
        lifecycle.start();

        histogram.record(EpochNanos.now() - TimeUnit.MILLISECONDS.toNanos(10));
        histogram.record(EpochNanos.now() - TimeUnit.MILLISECONDS.toNanos(10));
        Files.writeString(triggerPath, pass1Output.toString(), StandardCharsets.UTF_8);
        awaitDeleted(triggerPath);
        awaitWritten(pass1Output);
        Map<?, ?> firstWrite = objectMapper.readValue(pass1Output.toFile(), Map.class);
        assertThat(firstWrite.get("count")).isEqualTo(2);

        // 리셋 뒤 더 기록해서, 만약 재처리가 실제로 일어난다면(버그) 값이 달라질 조건을 만든다.
        histogram.record(EpochNanos.now() - TimeUnit.MILLISECONDS.toNanos(10));
        histogram.record(EpochNanos.now() - TimeUnit.MILLISECONDS.toNanos(10));
        histogram.record(EpochNanos.now() - TimeUnit.MILLISECONDS.toNanos(10));

        // 삭제 실패로 같은 트리거가 되살아난 상황 재현 — 같은 목적지 경로로 다시 트리거.
        Files.writeString(triggerPath, pass1Output.toString(), StandardCharsets.UTF_8);
        awaitDeleted(triggerPath);

        Map<?, ?> afterRepeat = objectMapper.readValue(pass1Output.toFile(), Map.class);
        assertThat(afterRepeat.get("count")).as("재처리로 스냅샷이 덮어써지면 안 된다").isEqualTo(2);
    }

    /**
     * 2b 리뷰 지적 ①④ — 드라이버가 아직 다 못 쓴(빈) 트리거를 폴링 스레드가 봐도 죽지 않고, 그
     * 트리거를 버린 뒤 이어지는 정상 트리거는 그대로 처리하는지 확인한다.
     */
    @Test
    @DisplayName("빈 트리거 파일을 만나도 폴링 스레드가 안 죽고 이어지는 정상 트리거는 처리한다")
    void 빈_트리거는_버리고_다음_트리거는_처리한다(@TempDir Path tempDir) throws Exception {
        LatencyHistogram histogram = new LatencyHistogram(true);
        Path finalOutput = tempDir.resolve("latency-v2-final.json");
        Path triggerPath = tempDir.resolve("latency-trigger-v2");
        Path pass1Output = tempDir.resolve("latency-v2-pass1.json");
        lifecycle = new AccountLatencyDumpLifecycle(histogram, finalOutput.toString(), triggerPath.toString());
        lifecycle.start();

        Files.writeString(triggerPath, "", StandardCharsets.UTF_8); // 드라이버가 아직 못 채운 상황을 흉내
        // 빈 내용은 handleTrigger가 그대로 두고 다음 폴을 기다린다 — 폴 주기(200ms)를 몇 번 넘겨도
        // 폴링 스레드가 안 죽었는지만 확인하고(트리거는 아직 그대로), 실제 내용으로 덮어써 이어간다.
        Thread.sleep(500);
        assertThat(lifecycle.isRunning()).isTrue();

        histogram.record(EpochNanos.now() - TimeUnit.MILLISECONDS.toNanos(10));
        Files.writeString(triggerPath, pass1Output.toString(), StandardCharsets.UTF_8);
        awaitDeleted(triggerPath);
        awaitWritten(pass1Output);

        Map<?, ?> written = objectMapper.readValue(pass1Output.toFile(), Map.class);
        assertThat(written.get("count")).isEqualTo(1);
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

    /**
     * 트리거를 claim(이동)하자마자 원본 경로가 사라지므로(부하 드라이버의 완료 신호), awaitDeleted가
     * 돌아온 시점에 목적지 파일 쓰기까지 끝났다는 보장은 엄밀히는 없다 — 실무에선 같은 메서드
     * 호출 안에서 곧바로 이어지는 동기 쓰기라 사실상 동시지만, 테스트에서는 목적지 파일 존재까지
     * 별도로 기다려 이 미세한 시차를 흡수한다.
     */
    private void awaitWritten(Path outputPath) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!Files.exists(outputPath)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("3초 안에 출력 파일이 안 써짐: " + outputPath);
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("대기 중 인터럽트됨", e);
            }
        }
    }
}
