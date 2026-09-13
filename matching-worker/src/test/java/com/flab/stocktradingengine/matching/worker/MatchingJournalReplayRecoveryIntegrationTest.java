package com.flab.stocktradingengine.matching.worker;

import static org.assertj.core.api.Assertions.assertThat;

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
 * 2c-2 핵심 — 매칭 워커가 재시작하면, 이전 프로세스가 Archive에 남긴 저널을 실제로 읽어
 * (listRecordingsForUri → replay → decode → recover) 호가창의 미체결 주문을 되살리는지
 * end-to-end로 확인한다. {@link MatchingJournalDurabilityAcrossRestartIntegrationTest}(2c-1,
 * "녹화가 남아있다"까지)의 다음 단계 — 이번엔 "그 녹화를 실제로 읽어서 호가창이 복원된다"까지
 * 증명한다. account-worker의 {@code AccountJournalReplayRecoveryIntegrationTest}와 같은 결이다.
 *
 * <p>복구가 no-op 리스너로만 진행됐는지(체결 재발행 0건)는 matching-disruptor의
 * {@code MatchingEngineRecoveryTest}가 이미 단위로 증명한다 — 여기서는 그 위에서 "실제 Archive
 * 리플레이 배선이 호가창을 맞게 되살리는지"만 end-to-end로 본다.</p>
 */
class MatchingJournalReplayRecoveryIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;

    @TempDir
    private Path archiveDir;

    @Test
    void 재기동하면_저널을_리플레이해서_미체결_주문을_복원한다() {
        ConfigurableApplicationContext run1 = launch();
        try {
            MatchingEngine engine = run1.getBean(MatchingEngine.class);

            // 상대 주문 없이 하나만 넣어 미체결(resting) 상태로 남긴다.
            engine.publishPlace(100L, 1L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now());
            awaitContainsOrder(engine, 100L);
        } finally {
            run1.close();
        }

        ConfigurableApplicationContext run2 = launch();
        try {
            MatchingEngine engine = run2.getBean(MatchingEngine.class);

            assertThat(engine.containsOrder(STOCK, 100L))
                .as("재시작 전 미체결 주문이 리플레이로 복원돼 있어야 한다")
                .isTrue();
        } finally {
            run2.close();
        }
    }

    private ConfigurableApplicationContext launch() {
        return new SpringApplicationBuilder(MatchingWorkerApplication.class)
            .web(WebApplicationType.NONE)
            .properties("matching.worker.archive-dir=" + archiveDir.toAbsolutePath())
            .run();
    }

    /** 매칭 소비자 스레드가 주문을 호가창에 반영할 때까지 기다린다. */
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
