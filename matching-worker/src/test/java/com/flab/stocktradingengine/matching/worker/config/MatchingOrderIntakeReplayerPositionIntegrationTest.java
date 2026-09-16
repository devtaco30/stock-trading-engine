package com.flab.stocktradingengine.matching.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.codec.OrderCodec;
import com.flab.stocktradingengine.matching.worker.MatchingWorkerApplication;
import com.flab.stocktradingengine.matching.worker.recovery.MatchingOrderIntakeReplayer;
import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;

/**
 * I2 U2b — {@link MatchingOrderIntakeReplayer#readFrom}이 실제로 지정한 발행자(sessionId)
 * position부터만 읽는지(그 앞 엔트리는 재생하지 않는지)를 반환된 엔트리 자체를 세어 증명한다.
 * {@code MatchingJournalReplayerPositionIntegrationTest}(matching.worker 패키지)와 같은 run1/run2
 * 2단계 구조 — Archive가 아직 멈추지 않은 자기 자신의 recording은 stopPosition이 NULL_POSITION이라
 * 리플레이할 수 없어, run1을 정상 종료해 확정 짓고 run2에서 읽는다.
 *
 * <p>주문 인테이크는 {@link MatchingOrderIntakeReplayer}가 recording을 세션(발행자)별로 따로
 * 취급하므로(I2 U2b LLD §3), 저널 리플레이 테스트처럼 recordingId 하나만 쓰지 않고
 * {@link Publication#sessionId()}를 위치 맵의 키로 쓴다 — 실제 배선({@link MatchingOrderIntakeConfig})과
 * 같은 채널·스트림에 이 테스트 전용 Publication을 열어
 * 주문을 흘려보낸다({@code MatchingOrderIntakeIntegrationTest}와 같은 방식).</p>
 */
class MatchingOrderIntakeReplayerPositionIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;

    private final OrderCodec codec = new OrderCodec();

    @TempDir
    private Path archiveDir;

    @Test
    void readFrom은_지정한_position_이전_엔트리를_재생하지_않는다() throws Exception {
        int sessionId;
        long positionAfterFirst;

        ConfigurableApplicationContext run1 = launch();
        try {
            Aeron aeron = run1.getBean(Aeron.class);
            AeronArchive archive = run1.getBean(AeronArchive.class);

            Publication publication = aeron.addPublication(
                MatchingOrderIntakeConfig.DEFAULT_INTAKE_CHANNEL, AeronStreamIds.MATCHING_INTAKE);
            try {
                awaitConnected(publication);
                sessionId = publication.sessionId();
                long recordingId = awaitRecordingId(archive, sessionId);

                send(publication, new JournaledOrder(
                    EventType.PLACE, 1L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now()));
                positionAfterFirst = awaitPositionAdvance(archive, recordingId, 0L);

                send(publication, new JournaledOrder(
                    EventType.PLACE, 2L, 200L, STOCK, OrderSide.BUY, new BigDecimal("9000"), 5, Instant.now()));
                send(publication, new JournaledOrder(
                    EventType.PLACE, 3L, 300L, STOCK, OrderSide.BUY, new BigDecimal("8000"), 3, Instant.now()));
                awaitPositionAdvance(archive, recordingId, positionAfterFirst);
            } finally {
                publication.close();
            }
        } finally {
            run1.close(); // 녹화를 멈춰 stopPosition을 확정 짓는다
        }

        ConfigurableApplicationContext run2 = launch();
        try {
            AeronArchive archive = run2.getBean(AeronArchive.class);
            MatchingOrderIntakeReplayer replayer = new MatchingOrderIntakeReplayer(archive);

            List<JournaledOrder> all = replayer.readFrom(
                MatchingOrderIntakeConfig.DEFAULT_INTAKE_CHANNEL, AeronStreamIds.MATCHING_INTAKE, Map.of());
            assertThat(all).as("맵에 이 세션 기록이 없으면 recording 시작 위치부터 전부 읽어야 한다")
                .extracting(JournaledOrder::orderId)
                .containsExactly(1L, 2L, 3L);

            List<JournaledOrder> fromFirst = replayer.readFrom(
                MatchingOrderIntakeConfig.DEFAULT_INTAKE_CHANNEL, AeronStreamIds.MATCHING_INTAKE,
                Map.of(sessionId, positionAfterFirst));
            assertThat(fromFirst)
                .as("이 세션의 position을 첫 엔트리 이후로 주면 1은 빠지고 2·3만 나와야 한다")
                .extracting(JournaledOrder::orderId)
                .containsExactly(2L, 3L);
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

    private void send(Publication publication, JournaledOrder order) {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        int length = codec.encode(buffer, 0, order);

        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        long result;
        do {
            result = publication.offer(buffer, 0, length);
            if (result > 0) {
                return;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 주문 발행 실패 — offer 반환=" + result);
            }
            Thread.yield();
        } while (true);
    }

    private void awaitConnected(Publication publication) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (!publication.isConnected()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 Publication이 연결되지 않음");
            }
            Thread.yield();
        }
    }

    private long awaitRecordingId(AeronArchive archive, int sessionId) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        long recordingId;
        while ((recordingId = findRecordingId(archive, sessionId)) == -1L) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 주문 인테이크 녹화가 시작되지 않음");
            }
            Thread.yield();
        }
        return recordingId;
    }

    private long findRecordingId(AeronArchive archive, int sessionId) {
        long[] found = {-1L};
        archive.listRecordingsForUri(0, 10,
            MatchingOrderIntakeConfig.DEFAULT_INTAKE_CHANNEL, AeronStreamIds.MATCHING_INTAKE,
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
             startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
             mtuLength, foundSessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
                if (foundSessionId == sessionId) {
                    found[0] = recordingId;
                }
            });
        return found[0];
    }

    /** 녹화 위치가 기준값보다 늘어날 때까지 기다린 뒤, 늘어난 위치를 돌려준다. */
    private long awaitPositionAdvance(AeronArchive archive, long recordingId, long positionBefore) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        long position;
        while ((position = archive.getRecordingPosition(recordingId)) <= positionBefore) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 녹화 위치가 늘지 않음");
            }
            Thread.yield();
        }
        return position;
    }
}
