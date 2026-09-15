package com.flab.stocktradingengine.account.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
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
import io.aeron.archive.client.AeronArchive;

/**
 * ADR-032, U4b 핵심 — "매칭이 냈지만 계좌가 재시작되기 전에 못 받은 체결"이 재기동 시 fill
 * Archive(6001) replay로 되살아나는지 end-to-end로 확인한다. {@link AccountJournalReplayRecoveryIntegrationTest}
 * (저널 replay, run1/close/run2 패턴)와 {@link AccountFillIntegrationTest}(6001 발행 방식)를
 * 합친 형태다.
 *
 * <h3>gap을 만드는 방법 — 타이밍 경쟁이 아니라 결정적으로</h3>
 * <p>run1에서 {@link AccountFillReceiver#close()}를 테스트가 직접 불러 폴 스레드를 먼저 멈춘 뒤에
 * 두 번째 체결을 발행한다 — "수신기가 마침 그 순간 못 받았을 수도 있다"는 race가 아니라, 발행
 * 시점에 수신기가 이미 죽어 있어 절대 못 받는다는 걸 보장한다. 녹화는 프로덕션이 담당한다(fork3
 * U3, {@link AccountFillIntakeConfig#accountFillRecordingSubscriptionId} — REMOTE, 컨텍스트 기동
 * 시 자동 시작) — 이 테스트는 매칭 역할을 하는 {@link Publication}만 열면 되고, 두 번째 체결은
 * 수신기가 죽어 있어도 그 REMOTE 녹화엔 그대로 남는다 — run2가 그 recording을 replay해서
 * 되살려야 한다.</p>
 *
 * <p>{@link AccountJournalReplayRecoveryIntegrationTest}(86718c3)처럼 run1을 라이브로 읽어
 * 오라클로 쓰지 않는다 — 두 체결의 예약증거금·미수금을 손으로 계산한 결정적 기준값과 대조한다.</p>
 */
class AccountFillReplayRecoveryIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;
    private static final long NOT_FOUND = -1L;
    // 매도 계좌는 이 워커가 소유하지 않은 값(999)으로 둔다 — 매도 쪽 보유·검증은 U4b의 관심사가
    // 아니고, 엔진이 ACCOUNT_NOT_FOUND로 알아서 격리해 매수 쪽 검증만 단순하게 볼 수 있다.
    private static final long SELL_ACCOUNT_ID = 999L;

    // 결정적 기준값 — seed 잔고 1,000,000 · margin-rate 0.40.
    // fill#1(10주 @ 10,000): 예약증거금 40,000 → 지급되어 잔고 -40,000, 나머지 60,000은 미수금.
    // fill#2(5주 @ 9,000): 예약증거금 18,000 → 지급되어 잔고 -18,000, 나머지 27,000은 미수금.
    // 둘 다 반영된 뒤: 잔고 1,000,000 - 40,000 - 18,000 = 942,000, 미수금 60,000 + 27,000 = 87,000,
    // 보유 10 + 5 = 15, 예약증거금은 둘 다 전량체결이라 0.
    private static final BigDecimal EXPECTED_BALANCE = new BigDecimal("942000");
    private static final BigDecimal EXPECTED_UNPAID = new BigDecimal("87000");
    private static final int EXPECTED_HOLDING = 15;

    private final FillCodec codec = new FillCodec();

    @TempDir
    private Path archiveDir;

    @Test
    void 재기동하면_수신기가_못받은_체결을_fill_replay로_복원한다() {
        ConfigurableApplicationContext run1 = launch();
        try {
            AccountEngine engine = run1.getBean(AccountEngine.class);
            AccountFillReceiver receiver = run1.getBean(AccountFillReceiver.class);
            Aeron aeron = run1.getBean(Aeron.class);
            AeronArchive aeronArchive = run1.getBean(AeronArchive.class);

            engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
            long buyOrderId1 = awaitOrderId(engine, "r1");

            // 프로덕션 REMOTE 녹화(AccountFillIntakeConfig.accountFillRecordingSubscriptionId)는
            // 컨텍스트 기동 때 이미 시작돼 있다 — 실제로 image가 붙어야(=발행자가 나타나야) 카탈로그에
            // recordingId가 생기므로, 이 테스트의 Publication을 만들고 연결을 기다린 뒤에야
            // awaitRecordingId가 성공한다.
            Publication publication = aeron.addPublication(AccountFillIntakeConfig.DEFAULT_FILL_CHANNEL, AeronStreamIds.FILL);
            try {
                awaitConnected(publication);
                awaitRecordingId(aeronArchive);

                // fill#1 — 라이브로 수신기가 받는다. buyOrderId는 r1이 실제로 발급받은 값이어야
                // AccountState의 예약과 매칭된다(엔진이 orderId로 예약을 찾는다).
                send(publication, new FilledTrade(9001L, STOCK, buyOrderId1, 1L, 101L, SELL_ACCOUNT_ID, 10, new BigDecimal("10000")));
                awaitHolding(engine, 10);

                // 수신기를 여기서 확실히 멈춘다 — 이후 발행되는 체결은 절대 라이브로 못 받는다(위 클래스
                // javadoc "결정적으로" 참고). run1.close()가 나중에 다시 close()를 불러도 안전하다(멱등).
                receiver.close();

                engine.publishBuy(1L, STOCK, new BigDecimal("9000"), 5, "r2");
                long buyOrderId2 = awaitOrderId(engine, "r2");

                // fill#2 — 수신기가 죽어 있어 라이브로는 못 받지만, 이 recording에는 남는다.
                send(publication, new FilledTrade(9002L, STOCK, buyOrderId2, 1L, 102L, SELL_ACCOUNT_ID, 5, new BigDecimal("9000")));
            } finally {
                publication.close();
            }
        } finally {
            run1.close(); // 저널·fill 녹화를 멈춰 stopPosition을 확정 짓고, 그 시점 스냅샷을 찍는다
        }

        ConfigurableApplicationContext run2 = launch();
        try {
            AccountEngine engine = run2.getBean(AccountEngine.class);
            AccountState state = engine.accountState(1L);

            assertThat(state.balance().compareTo(EXPECTED_BALANCE)).as("두 체결 다 반영된 잔고여야 한다").isZero();
            assertThat(state.reservedMargin().compareTo(BigDecimal.ZERO)).as("예약증거금이 전부 풀려야 한다").isZero();
            assertThat(state.unpaid().compareTo(EXPECTED_UNPAID)).as("두 체결 다 반영된 미수금이어야 한다").isZero();
            assertThat(state.holding(STOCK)).as("두 체결 다 반영된 보유 수량이어야 한다").isEqualTo(EXPECTED_HOLDING);
        } finally {
            run2.close();
        }
    }

    private ConfigurableApplicationContext launch() {
        return new SpringApplicationBuilder(AccountWorkerApplication.class)
            .web(WebApplicationType.NONE)
            .properties(
                "account-worker.seed-accounts[0].account-id=1",
                "account-worker.seed-accounts[0].balance=1000000",
                "account-worker.seed-accounts[0].margin-rate=0.40",
                "account.worker.archive-dir=" + archiveDir.toAbsolutePath())
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
            AccountFillIntakeConfig.DEFAULT_FILL_CHANNEL, AeronStreamIds.FILL,
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
             startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
             mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) ->
                found[0] = recordingId);
        return found[0];
    }

    /** requestId에 orderId가 발급될(=accept 처리가 끝날) 때까지 기다린 뒤 그 orderId를 돌려준다. */
    private long awaitOrderId(AccountEngine engine, String requestId) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        Long orderId;
        while ((orderId = engine.accountState(1L).orderIdFor(requestId)) == null) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 " + requestId + "의 orderId가 발급되지 않음");
            }
            Thread.yield();
        }
        return orderId;
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
}
