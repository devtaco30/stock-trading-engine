package com.flab.stocktradingengine.matching.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import com.flab.stocktradingengine.matching.disruptor.MatchingEngine;
import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.archive.client.AeronArchive;

/**
 * matching-worker 앱을 실제로 띄우고, 매칭 엔진에 들어온 주문이 저널 스트림(2005)으로 Aeron
 * Archive에 실제로 durable 녹화되는지 확인하는 end-to-end 테스트(2c-1). account-worker의
 * {@code AccountJournalArchiveRecordingIntegrationTest}와 같은 결이다.
 */
@SpringBootTest(classes = MatchingWorkerApplication.class)
@DirtiesContext
class MatchingJournalArchiveRecordingIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;
    private static final long NOT_FOUND = -1L;

    @Autowired
    private AeronArchive aeronArchive;

    @Autowired
    private MatchingEngine matchingEngine;

    @Test
    void 매칭_엔진_입력이_저널_스트림에_durable_녹화된다() {
        long recordingId = awaitRecordingId();
        assertThat(recordingId).as("저널 스트림 녹화가 시작돼 recordingId가 있어야 한다").isNotEqualTo(NOT_FOUND);
        long positionBefore = aeronArchive.getRecordingPosition(recordingId);

        matchingEngine.publishPlace(1L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now());

        awaitPositionAdvance(recordingId, positionBefore);
    }

    private long awaitRecordingId() {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        long recordingId;
        while ((recordingId = findRecordingId()) == NOT_FOUND) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 저널 스트림 녹화가 시작되지 않음");
            }
            Thread.yield();
        }
        return recordingId;
    }

    private long findRecordingId() {
        long[] found = {NOT_FOUND};
        aeronArchive.listRecordingsForUri(0, 10,
            MatchingJournalArchiveConfig.JOURNAL_CHANNEL, MatchingJournalArchiveConfig.JOURNAL_STREAM_ID,
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
             startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
             mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) ->
                found[0] = recordingId);
        return found[0];
    }

    private void awaitPositionAdvance(long recordingId, long positionBefore) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (aeronArchive.getRecordingPosition(recordingId) <= positionBefore) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 녹화 위치가 늘지 않음 — 저널 엔트리가 디스크에 기록되지 않은 것으로 보임");
            }
            Thread.yield();
        }
    }
}
