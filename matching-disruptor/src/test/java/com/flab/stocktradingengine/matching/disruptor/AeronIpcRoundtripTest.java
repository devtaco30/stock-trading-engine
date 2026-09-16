package com.flab.stocktradingengine.matching.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.ByteBuffer;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;

/**
 * Unit 4a — Aeron 배선 검증(최소 왕복).
 *
 * <p>주문을 매칭엔진에 나르는 전달 계층을 Aeron 으로 바꾸기 전에, 이 환경(Java 17·Gradle 9·Apple Silicon)에서
 * Aeron 이 실제로 기동하고 메시지 한 건이 오가는지부터 확인한다. 도메인 로직은 여기서 다루지 않는다 —
 * 순수 배선 스모크다. 실제 주문 흐름(Aeron 수신 → publishPlace → 매칭)은 이후 단위에서 붙인다.</p>
 *
 * <h3>구성</h3>
 * <ul>
 *   <li><b>MediaDriver</b>: Aeron 이 실제로 바이트를 나르는 엔진. 여기선 별도 프로세스가 아니라
 *       테스트 JVM 안에 임베디드로 띄운다({@link MediaDriver#launchEmbedded()} — 고유 임시 디렉터리를 만들어
 *       다른 드라이버와 충돌하지 않는다).</li>
 *   <li><b>채널</b>: {@code aeron:ipc} — 같은 머신 안 공유메모리 전송(UDP 아님). Unit 4a 는 IPC 로 시작한다.</li>
 *   <li><b>Publication</b>(보내는 쪽) / <b>Subscription</b>(받는 쪽): 같은 채널·streamId 로 연결된다.</li>
 * </ul>
 */
class AeronIpcRoundtripTest {

    private static final String CHANNEL = "aeron:ipc";
    private static final int STREAM_ID = 1001;
    private static final String MESSAGE = "hello-aeron";
    private static final long TIMEOUT_NANOS = 5_000_000_000L; // 5초

    @Test
    void IPC로_메시지_한_건이_왕복한다() {
        MediaDriver driver = MediaDriver.launchEmbedded(cleanEmbeddedMediaDriverContext());
        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));

        try (Publication publication = aeron.addPublication(CHANNEL, STREAM_ID);
             Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID)) {

            awaitConnected(publication);

            UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
            int length = buffer.putStringWithoutLengthAscii(0, MESSAGE);
            offerUntilAccepted(publication, buffer, length);

            String received = pollForMessage(subscription);
            assertEquals(MESSAGE, received);
        } finally {
            aeron.close();
            driver.close();
        }
    }

    /** Publication 이 소비자와 연결될 때까지(또는 타임아웃까지) 기다린다. */
    private void awaitConnected(Publication publication) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (!publication.isConnected()) {
            if (System.nanoTime() > deadline) {
                fail("5초 안에 Publication 이 연결되지 않음");
            }
            Thread.yield();
        }
    }

    /**
     * 메시지를 발행한다. Aeron 의 {@code offer} 는 백프레셔·미연결 상황에서 음수를 돌려주므로,
     * 받아들여질 때까지(또는 타임아웃까지) 재시도한다.
     */
    private void offerUntilAccepted(Publication publication, UnsafeBuffer buffer, int length) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        long result;
        do {
            result = publication.offer(buffer, 0, length);
            if (result > 0) {
                return;
            }
            if (System.nanoTime() > deadline) {
                fail("5초 안에 메시지 발행 실패 — offer 반환=" + result);
            }
            Thread.yield();
        } while (true);
    }

    /** Subscription 을 폴링해 첫 메시지를 조립해 돌려준다. Aeron 은 수신 스레드가 직접 폴링해야 데이터를 가져온다. */
    private String pollForMessage(Subscription subscription) {
        StringBuilder assembled = new StringBuilder();
        FragmentHandler handler = (buffer, offset, length, header) ->
            assembled.append(buffer.getStringWithoutLengthAscii(offset, length));

        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (assembled.length() == 0) {
            int fragments = subscription.poll(handler, 10);
            if (fragments == 0) {
                if (System.nanoTime() > deadline) {
                    fail("5초 안에 메시지를 수신하지 못함");
                }
                Thread.yield();
            }
        }
        return assembled.toString();
    }

    /** Aeron 드라이버 통신용 임시 디렉터리(aeron-*)를 시작·종료 시 지운다(I10) — 기본값은 안 지운다. */
    private static MediaDriver.Context cleanEmbeddedMediaDriverContext() {
        return new MediaDriver.Context().dirDeleteOnStart(true).dirDeleteOnShutdown(true);
    }
}
