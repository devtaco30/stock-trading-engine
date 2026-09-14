package com.flab.stocktradingengine.matching.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import com.flab.stocktradingengine.matching.worker.MatchingWorkerApplication;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.codec.OrderCodec;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;

/**
 * matching-worker 앱을 실제로 띄우고, 진짜 Aeron IPC로 교차 주문을 발신해 매칭까지 되는지
 * 확인하는 end-to-end 테스트(파이프라인 연결 ①). {@link MatchingOrderIntakeConfig}가 만든
 * MediaDriver·Aeron을 그대로 쓴다 — 같은 프로세스라 Aeron 빈을 재사용해 테스트용 Publication만
 * 새로 연다(account-worker {@code AccountOrderIntakeIntegrationTest}와 같은 결).
 *
 * <p>관측은 {@link MatchListener} 빈을 따로 추가하지 않고(프로덕션 {@code AccountFillPublisher}가
 * 유일한 MatchListener 빈이라는 전제를 지킨다), 체결 Aeron Archive 스트림({@link MatchingFillPublishConfig})의
 * 녹화 위치가 늘어나는지로 확인한다(ADR-032, U2 — 예전엔 Kafka {@code account-fills} 토픽으로
 * 확인했다). 체결 내용 디코딩 검증은 {@code MatchingFillArchiveRecordingIntegrationTest} 몫이라
 * 여기서는 "정말 체결까지 났다"만 본다.</p>
 *
 * <p>{@code @DirtiesContext} — 이 컨텍스트가 만든 임베디드 MediaDriver를 다음 테스트와 안 겹치게 한다.</p>
 */
@SpringBootTest(classes = MatchingWorkerApplication.class)
@DirtiesContext
class MatchingOrderIntakeIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;
    private static final long NOT_FOUND = -1L;

    private final OrderCodec codec = new OrderCodec();

    @Autowired
    private Aeron aeron;

    @Autowired
    private AeronArchive aeronArchive;

    @Test
    void Aeron_IPC로_들어온_교차_주문이_매칭돼_체결_스트림에_녹화된다() throws Exception {
        long runId = System.nanoTime();
        long buyAccountId = runId;
        long sellAccountId = runId + 1;
        long buyOrderId = runId + 2;
        long sellOrderId = runId + 3;

        long recordingId = awaitRecordingId();
        long positionBefore = aeronArchive.getRecordingPosition(recordingId);

        Publication publication =
            aeron.addPublication(MatchingOrderIntakeConfig.INTAKE_CHANNEL, MatchingOrderIntakeConfig.INTAKE_STREAM_ID);
        try {
            awaitConnected(publication);

            Instant now = Instant.now();
            send(publication, new JournaledOrder(
                EventType.PLACE, buyOrderId, buyAccountId, STOCK, OrderSide.BUY, new BigDecimal("10000"), 4, now));
            send(publication, new JournaledOrder(
                EventType.PLACE, sellOrderId, sellAccountId, STOCK, OrderSide.SELL, new BigDecimal("10000"), 4, now.plusMillis(1)));

            awaitPositionAdvance(recordingId, positionBefore);
        } finally {
            publication.close();
        }
    }

    private long awaitRecordingId() {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        long recordingId;
        while ((recordingId = findRecordingId()) == NOT_FOUND) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 체결 스트림 녹화가 시작되지 않음");
            }
            Thread.yield();
        }
        return recordingId;
    }

    private long findRecordingId() {
        long[] found = {NOT_FOUND};
        aeronArchive.listRecordingsForUri(0, 10,
            MatchingFillPublishConfig.FILL_CHANNEL, MatchingFillPublishConfig.FILL_STREAM_ID,
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
                throw new AssertionError("5초 안에 체결 녹화 위치가 늘지 않음 — 매칭이 안 된 것으로 보임");
            }
            Thread.yield();
        }
    }

    /** 주문을 인코딩해 Aeron으로 발행한다. 백프레셔면 받아들여질 때까지 재시도한다. */
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
}
