package com.flab.stocktradingengine.time;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 단계별 지연 등록부를 파일로 내보내는 실행기 — 트리거 파일로 라운드 경계마다 덤프, 1초마다 시계열,
 * 종료 시 안전망 덤프. 계측이 꺼져 있으면 스레드도 파일도 만들지 않는다.
 */
class StageDumpRunnerTest {

    private StageDumpRunner runner;

    @AfterEach
    void tearDown() {
        if (runner != null) {
            runner.stop();
        }
    }

    private static void awaitTrue(java.util.function.BooleanSupplier condition, String message) throws InterruptedException {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertTrue(condition.getAsBoolean(), message);
    }

    @Test
    void 트리거_파일이_오면_내용의_경로에_덤프를_쓰고_트리거를_지운다(@TempDir Path dir) throws Exception {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        registry.stage("A").recordNanos(5_000);
        registry.stage("B").recordNanos(9_000);
        Path trigger = dir.resolve("trigger");
        Path round1 = dir.resolve("round1.json");
        runner = new StageDumpRunner(registry, dir.resolve("final.json").toString(), trigger.toString(), "", "");

        runner.start();
        Files.writeString(trigger, round1.toString());

        awaitTrue(() -> !Files.exists(trigger) && Files.exists(round1), "덤프가 쓰이고 트리거가 지워져야 한다");
        String json = Files.readString(round1);
        assertTrue(json.contains("\"A\":{\"count\":1"), json);
        assertTrue(json.contains("\"B\":{\"count\":1"), json);
    }

    @Test
    void 덤프는_라운드마다_비워진다(@TempDir Path dir) throws Exception {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        registry.stage("A").recordNanos(5_000);
        Path trigger = dir.resolve("trigger");
        Path round1 = dir.resolve("r1.json");
        Path round2 = dir.resolve("r2.json");
        runner = new StageDumpRunner(registry, dir.resolve("final.json").toString(), trigger.toString(), "", "");
        runner.start();

        Files.writeString(trigger, round1.toString());
        awaitTrue(() -> !Files.exists(trigger) && Files.exists(round1), "1회차");
        Files.writeString(trigger, round2.toString());
        awaitTrue(() -> !Files.exists(trigger) && Files.exists(round2), "2회차");

        assertTrue(Files.readString(round2).contains("\"A\":{\"count\":0"), Files.readString(round2));
    }

    @Test
    void 시계열을_지정하면_1초마다_한_줄씩_덧붙인다(@TempDir Path dir) throws Exception {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        registry.stage("B").recordNanos(7_000);
        Path series = dir.resolve("series.csv");
        runner = new StageDumpRunner(registry, dir.resolve("final.json").toString(), "", "B", series.toString());

        runner.start();
        awaitTrue(() -> readLines(series).size() >= 3, "헤더 + 두 줄 이상");

        List<String> lines = readLines(series);
        assertEquals("epochSecond,count,p50Micros,p99Micros", lines.get(0));
        assertTrue(lines.get(1).split(",").length == 4, lines.get(1));
    }

    @Test
    void 종료하면_안전망_덤프를_최종_경로에_쓴다(@TempDir Path dir) throws Exception {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        registry.stage("A").recordNanos(5_000);
        Path finalPath = dir.resolve("final.json");
        runner = new StageDumpRunner(registry, finalPath.toString(), "", "", "");

        runner.start();
        runner.stop();

        assertTrue(Files.readString(finalPath).contains("\"A\":{\"count\":1"));
    }

    @Test
    void 계측이_꺼져_있으면_트리거를_건드리지_않고_파일도_만들지_않는다(@TempDir Path dir) throws Exception {
        StageLatencyRegistry registry = new StageLatencyRegistry(false);
        Path trigger = dir.resolve("trigger");
        Path finalPath = dir.resolve("final.json");
        Path series = dir.resolve("series.csv");
        runner = new StageDumpRunner(registry, finalPath.toString(), trigger.toString(), "B", series.toString());
        Files.writeString(trigger, dir.resolve("x.json").toString());

        runner.start();
        Thread.sleep(1300);
        runner.stop();

        assertTrue(Files.exists(trigger), "꺼져 있으면 트리거를 처리하지 않는다");
        assertFalse(Files.exists(finalPath));
        assertFalse(Files.exists(series));
        assertFalse(runner.isRunning());
    }

    private static List<String> readLines(Path path) {
        try {
            return Files.exists(path) ? Files.readAllLines(path) : List.of();
        } catch (IOException e) {
            return List.of();
        }
    }
}
