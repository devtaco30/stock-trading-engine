package com.flab.stocktradingengine.account.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import com.flab.stocktradingengine.account.disruptor.domain.AccountState;
import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.disruptor.io.AccountFillReceiver;
import com.flab.stocktradingengine.account.worker.AccountWorkerApplication;
import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.codec.FillCodec;
import com.flab.stocktradingengine.codec.FilledTrade;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;

/**
 * ADR-032 I1 U3 핵심 — 매칭 프로세스가 둘 이상일 때(recording이 세션별로 따로 생김,
 * {@link AccountFillMultiSessionRecordingIntegrationTest}가 실측한 전제) 재기동 복구가 양쪽
 * recording을 다 읽는지 end-to-end로 확인한다. {@link AccountFillReplayRecoveryIntegrationTest}
 * (단일 발행자 gap 복구)의 다음 단계 — 이번엔 발행자가 둘이라 recording도 둘이다.
 *
 * <h3>발행자 둘을 어떻게 만드는가</h3>
 * <p>같은 Aeron 클라이언트에서 {@code aeron:ipc}로 두 번 발행하면 세션을 공유해(참조 카운트)
 * sessionId가 같아진다 — {@link AccountFillMultiSessionRecordingIntegrationTest}가 먼저 겪은
 * 문제다. 그래서 매칭 프로세스 A·B를 각각 별도 {@link MediaDriver}+{@link Aeron} 클라이언트로
 * 만들어 udp로 발행한다 — 실제 크로스프로세스 상황(별도 매칭 프로세스)을 흉내낸다.</p>
 *
 * <h3>gap을 만드는 방법</h3>
 * <p>{@link AccountFillReplayRecoveryIntegrationTest}와 같다 — {@link AccountFillReceiver#close()}로
 * 폴 스레드를 먼저 멈춘 뒤에 나머지 체결을 보낸다. fill#1(매칭A)만 라이브로 받고, fill#2(매칭A,
 * 같은 recording의 뒷부분)·fill#3(매칭B, 아예 새 recording)은 수신기가 죽어 있어 라이브로는
 * 못 받는다 — run2가 두 recording을 각자 sessionId로 읽어 되살려야 한다. fill#3의 recording은
 * run1의 스냅샷 시점에 sessionId가 위치 맵에 없으므로, "맵에 없으면 그 recording의 시작
 * 위치부터"(LLD 4절 U3) 분기까지 같이 검증한다.</p>
 */
class AccountFillMultiSourceReplayRecoveryIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;
    private static final long SELL_ACCOUNT_ID = 999L;
    private static final long NODE_ID = 3L;

    // 결정적 기준값 — seed 잔고 1,000,000 · margin-rate 0.40, 매수 10주 @ 10,000을 3+4+3주로
    // 나눠(fill#1 라이브·fill#2 매칭A gap·fill#3 매칭B 새 recording) 전량체결.
    // 지급 증거금 = 10*10,000*0.40 = 40,000 → 잔고 960,000. 미수금 = 10*10,000*0.60 = 60,000.
    private static final BigDecimal EXPECTED_BALANCE = new BigDecimal("960000");
    private static final BigDecimal EXPECTED_UNPAID = new BigDecimal("60000");
    private static final int EXPECTED_HOLDING = 10;

    private final FillCodec codec = new FillCodec();

    @TempDir
    private Path archiveDir;

    @Test
    void 재기동하면_두_매칭_프로세스의_recording을_각자_sessionId로_읽어_복원한다() throws IOException {
        int fillPort;
        try (DatagramSocket socket = new DatagramSocket(0)) {
            fillPort = socket.getLocalPort();
        }
        String fillChannel = "aeron:udp?endpoint=localhost:" + fillPort;

        ConfigurableApplicationContext run1 = launch(fillChannel);
        try {
            AccountEngine engine = run1.getBean(AccountEngine.class);
            AccountFillReceiver receiver = run1.getBean(AccountFillReceiver.class);

            engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
            awaitSeq(engine, 1L); // r1 예약 accept — 이 엔진의 첫 발급이라 counter=1
            long buyOrderId = orderId(1);

            MediaDriver matchingDriverA = MediaDriver.launchEmbedded(cleanEmbeddedMediaDriverContext());
            try {
                Aeron matchingAeronA = Aeron.connect(new Aeron.Context().aeronDirectoryName(matchingDriverA.aeronDirectoryName()));
                try {
                    Publication publicationA = matchingAeronA.addPublication(fillChannel, AeronStreamIds.FILL);
                    try {
                        awaitConnected(publicationA);

                        // fill#1 — 매칭A, 라이브로 수신기가 받는다.
                        send(publicationA, new FilledTrade(9001L, STOCK, buyOrderId, 1L, 101L, SELL_ACCOUNT_ID, 3, new BigDecimal("10000")));
                        awaitHolding(engine, 3);

                        // 수신기를 여기서 확실히 멈춘다 — 이후 발행되는 체결은 절대 라이브로 못 받는다
                        // (AccountFillReplayRecoveryIntegrationTest 클래스 javadoc "결정적으로" 참고).
                        receiver.close();

                        // fill#2 — 매칭A의 같은 recording에, 수신기가 죽어 있어 라이브로는 못 받지만 남는다.
                        send(publicationA, new FilledTrade(9002L, STOCK, buyOrderId, 1L, 101L, SELL_ACCOUNT_ID, 4, new BigDecimal("10000")));
                    } finally {
                        publicationA.close();
                    }
                } finally {
                    matchingAeronA.close();
                }
            } finally {
                matchingDriverA.close();
            }

            // 매칭B — 별도 드라이버·클라이언트라 sessionId가 매칭A와 다르다, 아예 새 recording이 생긴다.
            // 스냅샷(run1 종료 시점)엔 이 sessionId가 없다 — "맵에 없으면 시작 위치부터" 분기를 이 recording이 탄다.
            MediaDriver matchingDriverB = MediaDriver.launchEmbedded(cleanEmbeddedMediaDriverContext());
            try {
                Aeron matchingAeronB = Aeron.connect(new Aeron.Context().aeronDirectoryName(matchingDriverB.aeronDirectoryName()));
                try {
                    Publication publicationB = matchingAeronB.addPublication(fillChannel, AeronStreamIds.FILL);
                    try {
                        awaitConnected(publicationB);

                        // fill#3 — 매칭B, 수신기가 이미 죽어 있어 라이브로는 못 받는다.
                        send(publicationB, new FilledTrade(9003L, STOCK, buyOrderId, 1L, 101L, SELL_ACCOUNT_ID, 3, new BigDecimal("10000")));
                    } finally {
                        publicationB.close();
                    }
                } finally {
                    matchingAeronB.close();
                }
            } finally {
                matchingDriverB.close();
            }
        } finally {
            run1.close(); // 두 recording 다 stopPosition을 확정 짓고, fill#1까지만 반영된 상태로 스냅샷을 찍는다
        }

        ConfigurableApplicationContext run2 = launch(fillChannel);
        try {
            AccountEngine engine = run2.getBean(AccountEngine.class);
            AccountState state = engine.accountState(1L);

            assertThat(state.balance().compareTo(EXPECTED_BALANCE)).as("매칭A·매칭B 체결이 전부 반영된 잔고여야 한다").isZero();
            assertThat(state.reservedMargin().compareTo(BigDecimal.ZERO)).as("전량체결이므로 예약증거금이 전부 풀려야 한다").isZero();
            assertThat(state.unpaid().compareTo(EXPECTED_UNPAID)).as("매칭A·매칭B 체결이 전부 반영된 미수금이어야 한다").isZero();
            assertThat(state.holding(STOCK)).as("매칭A·매칭B 체결이 전부 반영된 보유 수량이어야 한다").isEqualTo(EXPECTED_HOLDING);
        } finally {
            run2.close();
        }
    }

    private ConfigurableApplicationContext launch(String fillChannel) {
        return new SpringApplicationBuilder(AccountWorkerApplication.class)
            .web(WebApplicationType.NONE)
            .properties(
                "account-worker.seed-accounts[0].account-id=1",
                "account-worker.seed-accounts[0].balance=1000000",
                "account-worker.seed-accounts[0].margin-rate=0.40",
                "account.worker.archive-dir=" + archiveDir.toAbsolutePath(),
                "transport.fill.channel=" + fillChannel)
            .run();
    }

    /** 체결을 인코딩해 Aeron으로 발행한다. 백프레셔면 받아들여질 때까지 재시도한다. */
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

    /** 계좌 seq가 기대값 이상이 될 때까지 기다린다(비동기 소비자 스레드 처리 대기). */
    private void awaitSeq(AccountEngine engine, long expectedSeq) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (engine.accountState(1L).seq() < expectedSeq) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 seq가 " + expectedSeq + "에 도달하지 않음");
            }
            Thread.yield();
        }
    }

    /** AccountOrderIdGenerator의 결정론 규칙((nodeId&lt;&lt;53)|counter)으로 orderId를 예측한다. */
    private static long orderId(long counter) {
        return (NODE_ID << 53) | counter;
    }

    /** 보유 수량이 기대치에 도달할(=체결 반영이 끝날) 때까지 기다린다. */
    private void awaitHolding(AccountEngine engine, int expected) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (engine.accountState(1L).holding(STOCK) != expected) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 보유 수량이 " + expected + "에 도달하지 않음");
            }
            Thread.yield();
        }
    }

    /** Aeron 드라이버 통신용 임시 디렉터리(aeron-*)를 시작·종료 시 지운다(I10) — 기본값은 안 지운다. */
    private static MediaDriver.Context cleanEmbeddedMediaDriverContext() {
        return new MediaDriver.Context().dirDeleteOnStart(true).dirDeleteOnShutdown(true);
    }
}
