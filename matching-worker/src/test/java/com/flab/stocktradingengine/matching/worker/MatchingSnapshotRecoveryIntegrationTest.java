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
import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * 2d-1b 핵심 — 매칭 워커가 graceful shutdown 때 스냅샷을 찍고, 재기동 때 그 스냅샷(+ 스냅샷 이후
 * 저널 delta)으로 호가창을 복원하는지 end-to-end로 확인한다. {@link MatchingJournalReplayRecoveryIntegrationTest}
 * (2c-2, 저널 0부터 전부 리플레이)의 다음 단계 — 이번엔 스냅샷이 있으면 그걸 우선 쓴다.
 *
 * <p>스냅샷이 있어도 매 재시작마다 graceful stop이 다시 전체 상태를 스냅샷으로 찍으므로,
 * "스냅샷 이후 delta가 실제로 position부터만 읽히는지"는 이 블랙박스 테스트만으론 구분되지
 * 않는다(매칭이 멱등이라 0부터 다시 읽어도 최종 상태는 같다) — 그건
 * {@link MatchingJournalReplayerPositionIntegrationTest}가 리플레이어를 직접 불러 증명한다.
 * 여기서는 "스냅샷+복구 파이프라인 전체가 반복 재시작에도 상태를 안 잃는다"를 본다.</p>
 */
class MatchingSnapshotRecoveryIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;

    @TempDir
    private Path archiveDir;

    @Test
    void graceful_shutdown하면_스냅샷_파일이_생기고_재기동하면_미체결_주문이_복원된다() {
        ConfigurableApplicationContext run1 = launch();
        try {
            MatchingEngine engine = run1.getBean(MatchingEngine.class);
            engine.publishPlace(100L, 1L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now());
            awaitContainsOrder(engine, 100L);
        } finally {
            run1.close(); // SmartLifecycle.stop() 체인 끝에서 MatchingSnapshotLifecycle이 스냅샷을 찍는다
        }

        File snapshotFile = new File(archiveDir.toFile(), "matching-snapshot.dat");
        assertThat(snapshotFile).as("graceful shutdown 뒤 스냅샷 파일이 남아있어야 한다").exists();

        ConfigurableApplicationContext run2 = launch();
        try {
            MatchingEngine engine = run2.getBean(MatchingEngine.class);
            assertThat(engine.containsOrder(STOCK, 100L))
                .as("재시작 전 미체결 주문이 스냅샷으로 복원돼 있어야 한다")
                .isTrue();
        } finally {
            run2.close();
        }
    }

    @Test
    void 두_번의_정상_재시작을_거쳐도_누적된_미체결_주문이_전부_복원된다() {
        ConfigurableApplicationContext run1 = launch();
        try {
            MatchingEngine engine = run1.getBean(MatchingEngine.class);
            engine.publishPlace(100L, 1L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now());
            awaitContainsOrder(engine, 100L);
        } finally {
            run1.close();
        }

        ConfigurableApplicationContext run2 = launch();
        try {
            MatchingEngine engine = run2.getBean(MatchingEngine.class);
            assertThat(engine.containsOrder(STOCK, 100L)).isTrue(); // run1의 스냅샷으로 복원됨

            // 크로스 안 되는 다른 가격 주문을 run2에서 새로 추가한다.
            engine.publishPlace(200L, 2L, STOCK, OrderSide.BUY, new BigDecimal("9000"), 5, Instant.now());
            awaitContainsOrder(engine, 200L);
        } finally {
            run2.close(); // run2의 graceful stop이 100·200 둘 다 담긴 새 스냅샷을 찍는다
        }

        ConfigurableApplicationContext run3 = launch();
        try {
            MatchingEngine engine = run3.getBean(MatchingEngine.class);
            assertThat(engine.containsOrder(STOCK, 100L)).as("run1에서 만든 주문이 남아있어야 한다").isTrue();
            assertThat(engine.containsOrder(STOCK, 200L)).as("run2에서 추가한 주문도 남아있어야 한다").isTrue();
        } finally {
            run3.close();
        }
    }

    private ConfigurableApplicationContext launch() {
        return new SpringApplicationBuilder(MatchingWorkerApplication.class)
            .web(WebApplicationType.NONE)
            .properties("matching.worker.archive-dir=" + archiveDir.toAbsolutePath())
            .run();
    }

    private void awaitContainsOrder(MatchingEngine engine, long orderId) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (!engine.containsOrder(STOCK, orderId)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 주문이 호가창에 반영되지 않음: orderId=" + orderId);
            }
            Thread.yield();
        }
    }
}
