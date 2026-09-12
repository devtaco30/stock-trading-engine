package com.flab.stocktradingengine.matching.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.matching.disruptor.MatchingEngine;
import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;

/**
 * U2 — 저널에 손상 바이트(잘못된 type ordinal)가 섞여 있어도 리플레이가 정상 엔트리만 복구하는지
 * 확인한다. {@link MatchingJournalReplayerPositionIntegrationTest}와 같은 결(실 Archive
 * 녹화·replay) — account-worker {@code AccountJournalReplayerPoisonIntegrationTest}의 매칭 미러.
 *
 * <p>Aeron {@code Image.poll}은 FragmentHandler가 던진 예외를 내부에서 잡아 errorHandler로
 * 넘기고 리플레이 자체를 죽이지 않는다(소스로 확인) — 그래서 이 테스트는 "startup이 abort된다"를
 * red로 재현하진 않는다. 이 테스트가 실제로 증명하는 건 tryDecode 적용 후 손상 엔트리가 우리
 * 로거로 명시적으로 skip되고, 결과 리스트에서 깨끗이 빠진다는 것이다.</p>
 */
class MatchingJournalReplayerPoisonIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;

    @TempDir
    private Path archiveDir;

    @Test
    void 저널에_손상_type_바이트가_섞여도_replay가_예외없이_끝나고_정상엔트리만_복구한다() {
        ConfigurableApplicationContext run1 = launch();
        try {
            MatchingEngine engine = run1.getBean(MatchingEngine.class);
            ExclusivePublication publication = run1.getBean(ExclusivePublication.class);

            engine.publishPlace(1L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now());
            awaitContainsOrder(engine, 1L);

            offerPoisonFrame(publication);

            engine.publishPlace(2L, 200L, STOCK, OrderSide.BUY, new BigDecimal("9000"), 5, Instant.now());
            awaitContainsOrder(engine, 2L);
        } finally {
            run1.close(); // 녹화를 멈춰 stopPosition을 확정 짓는다
        }

        ConfigurableApplicationContext run2 = launch();
        try {
            AeronArchive archive = run2.getBean(AeronArchive.class);
            MatchingJournalReplayer replayer = new MatchingJournalReplayer(archive);

            List<JournaledOrder> entries = replayer.readAll(
                MatchingJournalArchiveConfig.JOURNAL_CHANNEL, MatchingJournalArchiveConfig.JOURNAL_STREAM_ID);

            assertThat(entries)
                .as("손상 엔트리는 skip되고 정상 엔트리(orderId 1·2)만 남아야 한다")
                .extracting(JournaledOrder::orderId)
                .containsExactly(1L, 2L);
        } finally {
            run2.close();
        }
    }

    /** type ordinal이 EventType 범위(0~1)를 벗어난 손상 프레임 하나를 저널 발행 스트림에 직접 심는다. */
    private void offerPoisonFrame(ExclusivePublication publication) {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[16]);
        buffer.putByte(0, (byte) 99);
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (publication.offer(buffer, 0, buffer.capacity()) < 0) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 손상 프레임을 발행하지 못함");
            }
            Thread.yield();
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
