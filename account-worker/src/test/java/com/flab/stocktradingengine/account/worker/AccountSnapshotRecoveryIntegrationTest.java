package com.flab.stocktradingengine.account.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import com.flab.stocktradingengine.account.disruptor.domain.AccountState;
import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;

/**
 * 2d-2b 핵심 — 계좌 워커가 graceful shutdown 때 스냅샷을 찍고, 재기동 때 그 스냅샷(+ 스냅샷 이후
 * 저널 delta)으로 계좌 상태를 복원하는지 end-to-end로 확인한다. {@link AccountJournalReplayRecoveryIntegrationTest}
 * (2b-2b, 저널 0부터 전부 리플레이)의 다음 단계 — 이번엔 스냅샷이 있으면 그걸 우선 쓴다. matching
 * {@code MatchingSnapshotRecoveryIntegrationTest}와 같은 결.
 */
class AccountSnapshotRecoveryIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;

    // 복구가 되살려야 할 결정적 기준값 — seed 잔고 1,000,000 · margin-rate 0.40 · 매수 10주 @ 10,000(예약 40,000),
    // 그중 4주만 부분체결. 지급 증거금 = 4 * 10,000 * 0.40 = 16,000 → 잔고 984,000, 미수금 = 4*10,000*0.60 = 24,000,
    // 남은 예약(미체결 6주분) = 24,000, 보유 4. run1을 라이브로 읽어 오라클로 쓰지 않는다 —
    // 그 읽기는 소비자 스레드와 경쟁해 반쪽 상태(holding만 갱신된 찰나)를 잡을 수 있어서다.
    private static final BigDecimal EXPECTED_BALANCE = new BigDecimal("984000");
    private static final BigDecimal EXPECTED_RESERVED = new BigDecimal("24000");
    private static final BigDecimal EXPECTED_UNPAID = new BigDecimal("24000");
    private static final int EXPECTED_HOLDING = 4;
    // orderIdFor 조회를 뺀 뒤(릭 수정 U2), AccountOrderIdGenerator의 결정론 규칙((nodeId<<53)|counter,
    // account-worker snowflake.node-id=3 기본값)으로 publishBuyFill에 넘길 orderId를 예측한다.
    private static final long NODE_ID = 3L;

    @TempDir
    private Path archiveDir;

    @Test
    void graceful_shutdown하면_스냅샷_파일이_생기고_재기동하면_계좌_상태가_복원된다() {
        ConfigurableApplicationContext run1 = launch();
        try {
            AccountEngine engine = run1.getBean(AccountEngine.class);

            engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
            awaitSeq(engine, 1L); // r1 예약 accept — 이 엔진의 첫 발급이라 counter=1
            long buyOrderId = orderId(1);

            engine.publishBuyFill(9001L, buyOrderId, 1L, STOCK, new BigDecimal("10000"), 4); // 부분체결
            // 체결 처리(→스냅샷에 담길 상태 확정)까지만 기다린다. 잔고·미수금을 여기서 라이브로 읽어
            // 오라클로 쓰지 않는다(위 상수 주석 참고) — run2가 되살린 값을 결정적 기준값에 대조한다.
            awaitHolding(engine, 4);
        } finally {
            run1.close(); // SmartLifecycle.stop() 체인 끝에서 AccountSnapshotLifecycle이 스냅샷을 찍는다
        }

        File snapshotFile = new File(archiveDir.toFile(), "account-snapshot.dat");
        assertThat(snapshotFile).as("graceful shutdown 뒤 스냅샷 파일이 남아있어야 한다").exists();

        ConfigurableApplicationContext run2 = launch();
        try {
            AccountEngine engine = run2.getBean(AccountEngine.class);
            AccountState state = engine.accountState(1L);

            assertThat(state.isDuplicateRequest("r1")).as("r1이 재시작 뒤에도 재전송 멱등 캐시에 남아있어야 한다").isTrue();
            assertThat(state.balance().compareTo(EXPECTED_BALANCE)).as("잔고가 복원돼야 한다").isZero();
            assertThat(state.reservedMargin().compareTo(EXPECTED_RESERVED)).as("예약증거금이 복원돼야 한다").isZero();
            assertThat(state.unpaid().compareTo(EXPECTED_UNPAID)).as("미수금이 복원돼야 한다").isZero();
            assertThat(state.holding(STOCK)).as("보유 수량이 복원돼야 한다").isEqualTo(EXPECTED_HOLDING);

            // 재시작 뒤에도 새 requestId가 정상 접수되는지 확인한다. 발급기 카운터가 스냅샷으로
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

    @Test
    void 두_번의_정상_재시작을_거쳐도_누적된_계좌_상태가_전부_복원된다() {
        ConfigurableApplicationContext run1 = launch();
        try {
            AccountEngine engine = run1.getBean(AccountEngine.class);
            engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
            awaitSeq(engine, 1L);
        } finally {
            run1.close();
        }

        ConfigurableApplicationContext run2 = launch();
        try {
            AccountEngine engine = run2.getBean(AccountEngine.class);
            assertThat(engine.accountState(1L).isDuplicateRequest("r1"))
                .as("run1의 스냅샷으로 복원돼 r1이 재전송 멱등 캐시에 남아있어야 한다").isTrue();

            long seqBeforeR2 = engine.accountState(1L).seq();
            engine.publishBuy(1L, STOCK, new BigDecimal("9000"), 3, "r2");
            awaitSeq(engine, seqBeforeR2 + 1);
        } finally {
            run2.close(); // run2의 graceful stop이 r1·r2 둘 다 담긴 새 스냅샷을 찍는다
        }

        ConfigurableApplicationContext run3 = launch();
        try {
            AccountEngine engine = run3.getBean(AccountEngine.class);
            AccountState state = engine.accountState(1L);
            assertThat(state.isDuplicateRequest("r1")).as("run1에서 처리된 r1이 남아있어야 한다").isTrue();
            assertThat(state.isDuplicateRequest("r2")).as("run2에서 처리된 r2도 남아있어야 한다").isTrue();
        } finally {
            run3.close();
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
