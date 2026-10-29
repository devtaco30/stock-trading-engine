package com.flab.stocktradingengine.matching.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.IntPredicate;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.aeron.StockSlotHasher;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.codec.OrderCodec;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.worker.MatchingWorkerApplication;
import com.flab.stocktradingengine.matching.worker.recovery.MatchingOrderIntakeReplayer;
import com.flab.stocktradingengine.matching.worker.recovery.MatchingSnapshotStore;
import com.flab.stocktradingengine.matching.worker.recovery.StoredMatchingSnapshot;
import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;

/**
 * 담당이 아니라 거부한 주문의 위치가 재시작 뒤에도 남는다(설계서 U5b 시험 3). 거부한 주문은 링버퍼에서 호가창에 반영되지
 * 않지만 위치만 담은 이벤트로 발행자별 반영 위치가 앞으로 나아가고, 정상 종료 스냅샷에 그 위치가 저장된다. 재시작 때 그 위치부터
 * 주문 입구 녹화를 읽으면 거부했던 주문이 다시 읽혀 적용되지 않는다.
 */
class MatchingOrderIntakeRejectedPositionRestartIntegrationTest {

    private static final StockSlotHasher HASHER = new StockSlotHasher(64);
    private static final String OWNED_STOCK = "005930";
    private static final String REJECTED_STOCK = otherSlotStock();
    private static final long TIMEOUT_NANOS = 8_000_000_000L;

    private final OrderCodec codec = new OrderCodec();

    @TempDir
    private Path archiveDir;

    /** 담당 판정을 REJECTED_STOCK의 종목 slot에서만 거짓으로 바꾼다. 같은 이름의 빈을 덮어쓴다. */
    @TestConfiguration
    static class RejectOneStockSlot {
        @Bean
        IntPredicate allStockSlotsOwned() {
            int rejectedSlot = HASHER.slotFor(REJECTED_STOCK);
            return slot -> slot != rejectedSlot;
        }
    }

    @Test
    void 거부한_주문의_위치까지_저장되어_재시작_뒤_녹화에서_그_주문이_다시_읽히지_않는다() throws Exception {
        int sessionId;

        ConfigurableApplicationContext run1 = launch();
        try {
            Aeron aeron = run1.getBean(Aeron.class);
            MatchingEngine engine = run1.getBean(MatchingEngine.class);
            Publication publication = aeron.addPublication(
                MatchingOrderIntakeConfig.DEFAULT_INTAKE_CHANNEL, AeronStreamIds.MATCHING_INTAKE);
            try {
                awaitUntil(publication::isConnected, "Publication이 연결되지 않음");
                sessionId = publication.sessionId();
                send(publication, order(1L, OWNED_STOCK));
                send(publication, order(2L, REJECTED_STOCK));
                long lastPosition = publication.position();

                awaitUntil(() -> Long.valueOf(lastPosition).equals(engine.lastAppliedOrderIntakePositions().get(sessionId)),
                    "거부한 주문까지 반영 위치가 올라가지 않음: " + engine.lastAppliedOrderIntakePositions());
            } finally {
                publication.close();
            }
        } finally {
            run1.close(); // 정상 종료 스냅샷에 위치가 저장되고 녹화가 멈춘다
        }

        ConfigurableApplicationContext run2 = launch();
        try {
            StoredMatchingSnapshot stored = run2.getBean(MatchingSnapshotStore.class).read().orElseThrow();
            AeronArchive archive = run2.getBean(AeronArchive.class);

            List<JournaledOrder> replayed = new MatchingOrderIntakeReplayer(archive).readFrom(
                MatchingOrderIntakeConfig.DEFAULT_INTAKE_CHANNEL, AeronStreamIds.MATCHING_INTAKE,
                stored.orderIntakePosition());

            assertThat(stored.orderIntakePosition()).containsKey(sessionId);
            assertThat(replayed).as("거부했던 주문 2도, 반영한 주문 1도 녹화에서 다시 읽히지 않아야 한다").isEmpty();
        } finally {
            run2.close();
        }
    }

    private ConfigurableApplicationContext launch() {
        return new SpringApplicationBuilder(MatchingWorkerApplication.class, RejectOneStockSlot.class)
            .web(WebApplicationType.NONE)
            .properties(
                "matching.worker.archive-dir=" + archiveDir.toAbsolutePath(),
                "spring.main.allow-bean-definition-overriding=true")
            .run();
    }

    private JournaledOrder order(long orderId, String stockCode) {
        return new JournaledOrder(EventType.PLACE, orderId, 100L + orderId, stockCode, OrderSide.BUY,
            new BigDecimal("10000"), 10, Instant.now());
    }

    private void send(Publication publication, JournaledOrder order) {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        int length = codec.encode(buffer, 0, order);
        long[] result = {0L};
        awaitUntil(() -> (result[0] = publication.offer(buffer, 0, length)) > 0, "주문 발행 실패 — offer 반환=" + result[0]);
    }

    private static void awaitUntil(java.util.function.BooleanSupplier condition, String failureMessage) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("8초 안에 조건이 충족되지 않음: " + failureMessage);
            }
            Thread.yield();
        }
    }

    private static String otherSlotStock() {
        for (int code = 1; code < 10_000; code++) {
            String candidate = String.format("%06d", code);
            if (HASHER.slotFor(candidate) != HASHER.slotFor("005930")) {
                return candidate;
            }
        }
        throw new IllegalStateException("종목 slot이 다른 종목을 찾지 못했다");
    }
}
