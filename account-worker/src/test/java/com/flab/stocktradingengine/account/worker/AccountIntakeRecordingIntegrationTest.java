package com.flab.stocktradingengine.account.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import com.flab.stocktradingengine.codec.AccountOrderCodec;
import com.flab.stocktradingengine.codec.DecodedAccountOrder;
import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;

/**
 * account-worker 앱을 실제로 띄우고, 인테이크 채널·스트림(4004)으로 들어온 주문이 Aeron Archive에
 * 실제로 durable 녹화되는지 확인하는 end-to-end 테스트(2a). "녹화가 된다"까지만 증명한다 — 리플레이는
 * 2b, 프로세스 재기동 사이 카탈로그 보존 정책은 이 유닛 범위 밖이다.
 *
 * <p>디스크에 실제로 쓰였다는 증거는 두 가지로 잡는다: ①이 채널·스트림으로
 * {@link AeronArchive#listRecordingsForUri}가 recordingId를 돌려준다(=녹화가 시작됐다) ②주문을
 * 보낸 뒤 {@link AeronArchive#getRecordingPosition}이 보내기 전보다 늘어난다(=바이트가 실제로
 * 기록됐다, 단순히 카탈로그 엔트리만 생긴 게 아니다).</p>
 *
 * <p>{@code @DirtiesContext} — 이 컨텍스트가 만든 ArchivingMediaDriver·archive 임시 디렉터리를
 * 다음 테스트와 안 겹치게 한다(archive 디렉터리 자체는 JVM 임시 디렉터리에 생성돼 OS가 정리한다).</p>
 */
@SpringBootTest(
    classes = AccountWorkerApplication.class,
    properties = {
        "account-worker.seed-accounts[0].account-id=1",
        "account-worker.seed-accounts[0].balance=1000000",
        "account-worker.seed-accounts[0].margin-rate=0.40"
    }
)
@DirtiesContext
class AccountIntakeRecordingIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;
    private static final int ORDER_COUNT = 5;
    private static final long NOT_FOUND = -1L;

    private final AccountOrderCodec codec = new AccountOrderCodec();

    @Autowired
    private Aeron aeron;

    @Autowired
    private AeronArchive aeronArchive;

    @Test
    void 인테이크로_들어온_주문이_Archive에_durable_녹화된다() throws InterruptedException {
        Publication publication =
            aeron.addPublication(AccountOrderIntakeConfig.INTAKE_CHANNEL, AccountOrderIntakeConfig.INTAKE_STREAM_ID);
        try {
            awaitConnected(publication);

            long recordingId = findRecordingId();
            assertThat(recordingId).as("녹화가 시작돼 recordingId가 있어야 한다").isNotEqualTo(NOT_FOUND);
            long positionBefore = aeronArchive.getRecordingPosition(recordingId);

            for (int i = 0; i < ORDER_COUNT; i++) {
                send(publication, new DecodedAccountOrder(
                    OrderSide.BUY, 1L, STOCK, new BigDecimal("10000"), 10, "r" + i));
            }

            awaitPositionAdvance(recordingId, positionBefore);
        } finally {
            publication.close();
        }
    }

    /** 인테이크 채널·스트림으로 진행 중인 녹화를 찾아 recordingId를 돌려준다. 없으면 {@link #NOT_FOUND}. */
    private long findRecordingId() {
        long[] found = {NOT_FOUND};
        aeronArchive.listRecordingsForUri(0, 10,
            AccountOrderIntakeConfig.INTAKE_CHANNEL, AccountOrderIntakeConfig.INTAKE_STREAM_ID,
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
             startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
             mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) ->
                found[0] = recordingId);
        return found[0];
    }

    /** 녹화 위치(디스크에 실제로 쓴 바이트 오프셋)가 기준값보다 늘어날 때까지 기다린다. */
    private void awaitPositionAdvance(long recordingId, long positionBefore) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (aeronArchive.getRecordingPosition(recordingId) <= positionBefore) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 녹화 위치가 늘지 않음 — 주문이 디스크에 기록되지 않은 것으로 보임");
            }
            Thread.yield();
        }
    }

    /** 주문을 인코딩해 Aeron으로 발행한다. 백프레셔면 받아들여질 때까지 재시도한다. */
    private void send(Publication publication, DecodedAccountOrder order) {
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
