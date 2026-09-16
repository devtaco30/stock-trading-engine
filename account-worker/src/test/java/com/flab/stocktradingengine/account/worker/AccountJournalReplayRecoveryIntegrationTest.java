package com.flab.stocktradingengine.account.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import com.flab.stocktradingengine.account.disruptor.domain.AccountState;
import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.worker.support.NoOpMatchingOrderSenderTestConfig;

/**
 * 2b-2b 핵심 — 계좌 워커가 재시작하면, 이전 프로세스가 Archive에 남긴 저널을 실제로 읽어
 * (listRecordingsForUri → replay → decode → recover) 잔고·보유·미수금·dedup·orderId 발급기까지
 * 되살리는지 end-to-end로 확인한다. {@link AccountJournalDurabilityAcrossRestartIntegrationTest}
 * (2b-1b, "녹화가 남아있다"까지)의 다음 단계 — 이번엔 "그 녹화를 실제로 읽어서 상태가 복원된다"까지
 * 증명한다.
 *
 * <p>{@code account.worker.archive-dir}를 고정 경로(이 테스트 메서드 동안만 사는 {@code @TempDir})로
 * 줘서 서로 다른 두 Spring 컨텍스트(재시작을 흉내)가 같은 Archive 카탈로그를 공유하게 한다.</p>
 */
class AccountJournalReplayRecoveryIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;

    // 복구가 되살려야 할 결정적 기준값 — seed 잔고 1,000,000 · margin-rate 0.40 · 매수 10주 @ 10,000.
    // 예약증거금 = 10 * 10,000 * 0.40 = 40,000. 체결로 그 40,000이 실제 지급되어 잔고 -= 40,000(→960,000),
    // 나머지 60,000은 미수금(T+2), 예약은 0으로 풀린다. run1을 라이브로 읽어 오라클로 쓰지 않는다 —
    // 그 읽기는 소비자 스레드와 경쟁해 반쪽 상태(holding만 갱신된 찰나)를 잡을 수 있어서다.
    private static final BigDecimal EXPECTED_BALANCE = new BigDecimal("960000");
    private static final BigDecimal EXPECTED_UNPAID = new BigDecimal("60000");
    private static final int EXPECTED_HOLDING = 10;
    // orderIdFor 조회를 뺀 뒤(릭 수정 U2), AccountOrderIdGenerator의 결정론 규칙((nodeId<<53)|counter,
    // account-worker snowflake.node-id=3 기본값)으로 publishBuyFill에 넘길 orderId를 예측한다.
    private static final long NODE_ID = 3L;

    @TempDir
    private Path archiveDir;

    @Test
    void 재기동하면_저널을_리플레이해서_잔고와_dedup을_복원한다() {
        ConfigurableApplicationContext run1 = launch();
        try {
            AccountEngine engine = run1.getBean(AccountEngine.class);

            engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
            awaitSeq(engine, 1L); // r1 예약 accept — 이 엔진의 첫 발급이라 counter=1
            long buyOrderId = orderId(1);

            engine.publishBuyFill(9001L, buyOrderId, 1L, STOCK, new BigDecimal("10000"), 10);
            // 체결이 처리(→저널에 durable 기록)될 때까지만 기다린다. 잔고·미수금을 여기서 라이브로
            // 읽어 오라클로 쓰지 않는다(위 상수 주석 참고) — run2가 되살린 값을 결정적 기준값에 대조한다.
            awaitHolding(engine, 10);
        } finally {
            run1.close();
        }

        ConfigurableApplicationContext run2 = launch();
        try {
            AccountEngine engine = run2.getBean(AccountEngine.class);
            AccountState state = engine.accountState(1L);

            assertThat(state.isDuplicateRequest("r1")).as("r1이 재시작 뒤에도 재전송 멱등 캐시에 남아있어야 한다").isTrue();
            assertThat(state.balance().compareTo(EXPECTED_BALANCE)).as("잔고가 복원돼야 한다").isZero();
            assertThat(state.reservedMargin().compareTo(BigDecimal.ZERO)).as("예약증거금이 복원돼야 한다").isZero();
            assertThat(state.unpaid().compareTo(EXPECTED_UNPAID)).as("미수금이 복원돼야 한다").isZero();
            assertThat(state.holding(STOCK)).as("보유 수량이 복원돼야 한다").isEqualTo(EXPECTED_HOLDING);

            // 재시작 뒤에도 새 requestId가 정상 접수되는지 확인한다. 발급기 카운터가 리플레이로
            // 정확히 이어지는지(옛 값과 안 겹치는지)는 리스너로 orderId를 직접 관측할 수 있는
            // AccountEngineRecoveryTest.복구_뒤_신규_주문은_카운터를_이어받는다에서 이미 검증한다 —
            // 이 블랙박스 테스트엔 orderId를 관측할 통로가 없다(릭 수정 U2로 조회 API 자체가 빠짐).
            long seqBeforeR2 = state.seq();
            engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 5, "r2");
            awaitSeq(engine, seqBeforeR2 + 1);
        } finally {
            run2.close();
        }
    }

    private ConfigurableApplicationContext launch() {
        return new SpringApplicationBuilder(AccountWorkerApplication.class)
            .web(WebApplicationType.NONE)
            .sources(NoOpMatchingOrderSenderTestConfig.class)
            .properties(
                "account-worker.seed-accounts[0].account-id=1",
                "account-worker.seed-accounts[0].balance=1000000",
                "account-worker.seed-accounts[0].margin-rate=0.40",
                "account.worker.archive-dir=" + archiveDir.toAbsolutePath())
            .run();
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
}
