package com.flab.stocktradingengine.matching.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import com.flab.stocktradingengine.codec.FillCodec;
import com.flab.stocktradingengine.codec.FilledTrade;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.worker.MatchingWorkerApplication;
import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;

/**
 * matching-worker 앱을 실제로 띄우고, 교차 주문이 체결되면 그 체결이 Aeron 스트림(6001)으로
 * 실제 durable 녹화되는지 + 그 녹화를 replay·decode하면 원래 체결과 같은지 확인하는 end-to-end
 * 테스트(ADR-032, U2). {@code MatchingJournalArchiveRecordingIntegrationTest}와 같은 결이되,
 * 저널과 달리 이 스트림은 replay/recover 빈이 없어(계좌 축 복구용, 유닛 4) 이 테스트가 직접
 * 저수준으로 replay한다({@code MatchingJournalReplayer.replayOne}과 같은 방식).
 *
 * <h3>왜 run1/run2 두 단계인가</h3>
 * <p>Archive는 아직 멈추지 않은(같은 프로세스가 계속 쓰고 있는) 녹화를 리플레이할 수 없다
 * (stopPosition이 NULL_POSITION). run1을 정상 종료해 녹화를 확정 짓고, 그 카탈로그를 공유하는
 * run2에서 리플레이한다({@code MatchingJournalReplayerPositionIntegrationTest}와 같은 이유).</p>
 */
class MatchingFillArchiveRecordingIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;
    private static final long NOT_FOUND = -1L;
    private static final String REPLAY_CHANNEL = "aeron:ipc";
    private static final int REPLAY_STREAM_ID = 6002;
    private static final int FRAGMENT_LIMIT = 10;

    @TempDir
    private Path archiveDir;

    @Test
    void 체결이_스트림에_녹화되고_replay하면_원래_체결과_같다() {
        long buyOrderId = 1L;
        long buyAccountId = 100L;
        long sellOrderId = 2L;
        long sellAccountId = 200L;
        long recordingId;

        ConfigurableApplicationContext run1 = launch();
        try {
            MatchingEngine engine = run1.getBean(MatchingEngine.class);
            AeronArchive archive = run1.getBean(AeronArchive.class);

            recordingId = awaitRecordingId(archive);
            long positionBefore = archive.getRecordingPosition(recordingId);

            Instant now = Instant.now();
            engine.publishPlace(buyOrderId, buyAccountId, STOCK, OrderSide.BUY, new BigDecimal("10000"), 4, now);
            engine.publishPlace(sellOrderId, sellAccountId, STOCK, OrderSide.SELL, new BigDecimal("10000"), 4, now.plusMillis(1));

            awaitPositionAdvance(archive, recordingId, positionBefore);
        } finally {
            run1.close(); // 녹화를 멈춰 stopPosition을 확정 짓는다
        }

        ConfigurableApplicationContext run2 = launch();
        try {
            AeronArchive archive = run2.getBean(AeronArchive.class);
            List<FilledTrade> replayed = replayAll(archive, recordingId);

            assertThat(replayed).hasSize(1);
            FilledTrade trade = replayed.get(0);
            assertThat(trade.stockCode()).isEqualTo(STOCK);
            assertThat(trade.buyOrderId()).isEqualTo(buyOrderId);
            assertThat(trade.buyAccountId()).isEqualTo(buyAccountId);
            assertThat(trade.sellOrderId()).isEqualTo(sellOrderId);
            assertThat(trade.sellAccountId()).isEqualTo(sellAccountId);
            assertThat(trade.filledQuantity()).isEqualTo(4);
            assertThat(trade.matchPrice()).isEqualByComparingTo("10000");
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

    private long awaitRecordingId(AeronArchive archive) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        long recordingId;
        while ((recordingId = findRecordingId(archive)) == NOT_FOUND) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 체결 스트림 녹화가 시작되지 않음");
            }
            Thread.yield();
        }
        return recordingId;
    }

    private long findRecordingId(AeronArchive archive) {
        long[] found = {NOT_FOUND};
        archive.listRecordingsForUri(0, 10,
            MatchingFillPublishConfig.FILL_CHANNEL, MatchingFillPublishConfig.FILL_STREAM_ID,
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
             startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
             mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) ->
                found[0] = recordingId);
        return found[0];
    }

    private void awaitPositionAdvance(AeronArchive archive, long recordingId, long positionBefore) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (archive.getRecordingPosition(recordingId) <= positionBefore) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 녹화 위치가 늘지 않음 — 체결이 디스크에 기록되지 않은 것으로 보임");
            }
            Thread.yield();
        }
    }

    private List<FilledTrade> replayAll(AeronArchive archive, long recordingId) {
        FillCodec codec = new FillCodec();
        List<FilledTrade> out = new ArrayList<>();
        long length = archive.getStopPosition(recordingId);
        try (Subscription subscription = archive.replay(
                recordingId, 0L, length, REPLAY_CHANNEL, REPLAY_STREAM_ID)) {
            awaitConnected(subscription);
            Image image = subscription.imageAtIndex(0);
            FragmentHandler handler = (buffer, offset, fragmentLength, header) -> {
                DirectBuffer directBuffer = buffer;
                out.add(codec.decode(directBuffer, offset));
            };
            while (!image.isEndOfStream() && !image.isClosed()) {
                image.poll(handler, FRAGMENT_LIMIT);
            }
        }
        return out;
    }

    private void awaitConnected(Subscription subscription) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (!subscription.isConnected()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("체결 리플레이 구독이 5초 안에 연결되지 않았습니다");
            }
            Thread.yield();
        }
    }
}
