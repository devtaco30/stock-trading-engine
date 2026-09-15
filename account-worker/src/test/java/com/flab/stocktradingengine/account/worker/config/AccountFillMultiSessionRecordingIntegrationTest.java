package com.flab.stocktradingengine.account.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import com.flab.stocktradingengine.account.worker.AccountWorkerApplication;
import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.codec.FillCodec;
import com.flab.stocktradingengine.codec.FilledTrade;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;

/**
 * 1-5 "미확인 전제" 실측 — 서로 다른 프로세스(매칭)가 같은 채널·스트림으로 체결을 보낼 때, Aeron
 * Archive의 REMOTE 녹화({@link AccountFillIntakeConfig} 클래스 javadoc)가 recording을 발행자
 * 세션별로 따로 만드는지, 하나로 합치는지를 코드로 확인한다(문서상 "recording은 세션 단위"라
 * 되어 있으나 이 프로젝트 코드로는 확인한 적이 없다, {@code docs/_v2_remaining_work_design.html} ⑤ 1-5).
 *
 * <h3>왜 udp인가 — ipc는 세션을 공유해 전제를 틀리게 만든다</h3>
 * <p>처음엔 같은 {@link Aeron} 클라이언트에서 {@code aeron:ipc}로 addPublication을 두 번 불러
 * 시도했는데, 같은 채널·스트림에 session-id를 안 박으면 드라이버가 기존 IPC 발행을 그대로
 * 공유해(참조 카운트) 두 Publication의 {@code sessionId()}가 같게 나왔다 — "발행자 세션이 다르다"는
 * 전제 자체가 깨졌다(별도 사실 확인, 이 테스트 파일 이전 버전에서 발견). udp는 이 공유가 없다
 * — {@link CrossShardFillFanoutIntegrationTest}처럼 별도 {@link MediaDriver}(매칭 역할)에서
 * udp로 발행해, 실제 크로스프로세스 상황(별도 매칭 프로세스가 재시작해 새 세션으로 다시 붙는
 * 경우)을 흉내낸다.</p>
 */
@SpringBootTest(classes = AccountWorkerApplication.class)
@DirtiesContext
class AccountFillMultiSessionRecordingIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;

    private static int fillPort;

    private final FillCodec codec = new FillCodec();

    @Autowired
    private AeronArchive aeronArchive;

    @DynamicPropertySource
    static void fillChannel(DynamicPropertyRegistry registry) throws IOException {
        try (DatagramSocket socket = new DatagramSocket(0)) {
            fillPort = socket.getLocalPort();
        }
        registry.add("transport.fill.channel", () -> "aeron:udp?endpoint=localhost:" + fillPort);
        registry.add("account-worker.seed-accounts[0].account-id", () -> "1");
        registry.add("account-worker.seed-accounts[0].balance", () -> "1000000");
        registry.add("account-worker.seed-accounts[0].margin-rate", () -> "0.40");
    }

    @Test
    void 서로_다른_프로세스가_발행하면_recording이_세션별로_따로_생기는지_실측() {
        String fillChannel = "aeron:udp?endpoint=localhost:" + fillPort;

        MediaDriver matchingDriverA = MediaDriver.launchEmbedded();
        MediaDriver matchingDriverB = MediaDriver.launchEmbedded();
        try {
            Aeron matchingAeronA = Aeron.connect(new Aeron.Context().aeronDirectoryName(matchingDriverA.aeronDirectoryName()));
            Aeron matchingAeronB = Aeron.connect(new Aeron.Context().aeronDirectoryName(matchingDriverB.aeronDirectoryName()));
            try {
                Publication publicationA = matchingAeronA.addPublication(fillChannel, AeronStreamIds.FILL);
                Publication publicationB = matchingAeronB.addPublication(fillChannel, AeronStreamIds.FILL);
                try {
                    assertThat(publicationA.sessionId()).as("별도 프로세스(드라이버)의 발행이므로 세션이 달라야 실측 전제가 성립한다")
                        .isNotEqualTo(publicationB.sessionId());

                    awaitConnected(publicationA);
                    awaitConnected(publicationB);

                    send(publicationA, new FilledTrade(9001L, STOCK, 1L, 1L, 2L, 2L, 1, new BigDecimal("10000")));
                    send(publicationB, new FilledTrade(9002L, STOCK, 1L, 1L, 2L, 2L, 1, new BigDecimal("10000")));

                    Set<Long> recordingIds = awaitRecordingCount(fillChannel, 2);

                    assertThat(recordingIds).as(
                        "1-5 실측 결과: 서로 다른 프로세스가 같은 채널·스트림에 발행하면 Aeron Archive REMOTE 녹화가 "
                            + "세션(=발행자 프로세스)별로 별도 recording을 만든다 — AccountFillReplayer·AccountFillReceiver의 "
                            + "단일 recording 가정(javadoc \"단순화\")이 실제로 깨지는 상황이 있다는 뜻")
                        .hasSize(2);
                } finally {
                    publicationA.close();
                    publicationB.close();
                }
            } finally {
                matchingAeronA.close();
                matchingAeronB.close();
            }
        } finally {
            matchingDriverA.close();
            matchingDriverB.close();
        }
    }

    private void send(Publication publication, FilledTrade trade) {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        int length = codec.encode(buffer, 0, trade);

        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        long result;
        do {
            result = publication.offer(buffer, 0, length);
            if (result > 0) {
                return;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 체결 발행 실패 — offer 반환=" + result);
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

    /** recording 카탈로그에 최소 {@code expectedAtLeast}개가 나타날 때까지 기다려 현재 recordingId 집합을 돌려준다. */
    private Set<Long> awaitRecordingCount(String fillChannel, int expectedAtLeast) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        Set<Long> found;
        do {
            found = listRecordingIds(fillChannel);
            if (found.size() >= expectedAtLeast) {
                return found;
            }
            if (System.nanoTime() > deadline) {
                return found; // 타임아웃이어도 실측 결과를 그대로 돌려준다 — 그다음 assertion이 결과를 드러낸다.
            }
            Thread.onSpinWait();
        } while (true);
    }

    private Set<Long> listRecordingIds(String fillChannel) {
        Set<Long> ids = new HashSet<>();
        aeronArchive.listRecordingsForUri(0, 10,
            fillChannel, AeronStreamIds.FILL,
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
             startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
             mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) ->
                ids.add(recordingId));
        return ids;
    }
}
