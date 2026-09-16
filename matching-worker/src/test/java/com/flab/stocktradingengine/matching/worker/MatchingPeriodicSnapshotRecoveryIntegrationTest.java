package com.flab.stocktradingengine.matching.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.worker.recovery.MatchingSnapshotWriter;
import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * I6 U3 — 이 작업의 증명. {@link MatchingSnapshotRecoveryIntegrationTest}(graceful stop 스냅샷)와
 * 달리, run1을 {@code matching.worker.graceful-snapshot-enabled=false}로 띄워 종료 시점 스냅샷을
 * 아예 안 찍는다 — 저널 N건(=10,000)째에 소비자 스레드가 스스로 찍는 러닝 중 스냅샷(I6 U1~U2)
 * 만으로 재기동 복구가 되는지를 본다.
 *
 * <h3>왜 "종료 안 함"이 아니라 "graceful 스냅샷만 끔"인가</h3>
 * <p>실제 크래시(kill -9)를 흉내내려면 별도 프로세스를 띄워 강제 종료해야 하는데, 이 저장소의
 * 다른 통합 테스트는 전부 같은 JVM 안에서 Spring 컨텍스트를 열고 닫는 방식이다. 같은 JVM에서
 * Aeron Archive 리소스(포트·파일 락)를 정리하지 않고 다음 컨텍스트를 띄우면 충돌한다. 그래서
 * "정상 종료를 거치지 않고(= 스냅샷을 종료 시점에 안 찍고)"(I6 LLD U3)를 문자 그대로 —
 * graceful-stop 스냅샷 빈 자체를 컨텍스트에서 빼는 방식으로 만든다. 컨텍스트 자체는 정상
 * 종료되지만(다른 리소스는 깨끗이 정리), "종료 시점에 최종 스냅샷을 찍는" 동작만 없다.</p>
 */
class MatchingPeriodicSnapshotRecoveryIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TARGET_ORDER_ID = 1L;
    // MatchingEventHandler.SNAPSHOT_INTERVAL_JOURNAL_ENTRIES와 같은 값(I6 D1) — 저널 이만큼째에
    // 소비자 스레드가 러닝 중 스냅샷을 정확히 한 번 찍는다.
    private static final long SNAPSHOT_INTERVAL = 10_000L;
    private static final long TIMEOUT_NANOS = 15_000_000_000L;

    @TempDir
    private Path archiveDir;

    @Test
    void graceful_스냅샷_없이_주기_스냅샷만으로_재기동해도_호가창이_복원된다() {
        ConfigurableApplicationContext run1 = launch(false);
        try {
            MatchingEngine engine = run1.getBean(MatchingEngine.class);
            MatchingSnapshotWriter writer = run1.getBean(MatchingSnapshotWriter.class);

            // 첫 이벤트 = 복원 여부를 확인할 실제 주문. 나머지 (N-1)건은 존재하지 않는 주문의
            // 취소 — 부작용 없이 저널 적용 순번만 정확히 N까지 밀어 올린다(handler U1 테스트와 같은 방식).
            engine.publishPlace(TARGET_ORDER_ID, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now());
            for (long i = 1; i < SNAPSHOT_INTERVAL; i++) {
                engine.publishCancel(999_999L, STOCK);
            }

            // durableSeq가 N에 도달 = 전용 쓰기 스레드가 실제로 fsync까지 끝냈다는 결정적 증거.
            awaitDurableSeq(writer, SNAPSHOT_INTERVAL);
        } finally {
            run1.close(); // graceful-snapshot-enabled=false라 MatchingSnapshotLifecycle 빈이 없다 — 종료 시점 스냅샷 없음
        }

        File snapshotFile = new File(archiveDir.toFile(), "matching-snapshot.dat");
        assertThat(snapshotFile)
            .as("graceful 스냅샷 없이도 주기 스냅샷이 디스크에 남아있어야 한다")
            .exists();

        ConfigurableApplicationContext run2 = launch(true);
        try {
            MatchingEngine engine = run2.getBean(MatchingEngine.class);
            assertThat(engine.containsOrder(STOCK, TARGET_ORDER_ID))
                .as("run1의 graceful 스냅샷 없이, 주기 스냅샷만으로 미체결 주문이 복원돼야 한다")
                .isTrue();
        } finally {
            run2.close();
        }
    }

    private ConfigurableApplicationContext launch(boolean gracefulSnapshotEnabled) {
        return new SpringApplicationBuilder(MatchingWorkerApplication.class)
            .web(WebApplicationType.NONE)
            .properties(
                "matching.worker.archive-dir=" + archiveDir.toAbsolutePath(),
                "matching.worker.graceful-snapshot-enabled=" + gracefulSnapshotEnabled)
            .run();
    }

    private void awaitDurableSeq(MatchingSnapshotWriter writer, long expected) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (writer.durableSeq() < expected) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("15초 안에 러닝 중 스냅샷 durableSeq가 " + expected
                    + "에 도달하지 않음(현재 " + writer.durableSeq() + ") — 주기 트리거가 도는지 확인할 것");
            }
            Thread.yield();
        }
    }
}
