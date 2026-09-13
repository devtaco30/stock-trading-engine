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

    @TempDir
    private Path archiveDir;

    @Test
    void 재기동하면_저널을_리플레이해서_잔고와_dedup을_복원한다() {
        long buyOrderId;
        BigDecimal balanceAfterFill;
        BigDecimal reservedMarginAfterFill;
        BigDecimal unpaidAfterFill;
        int holdingAfterFill;

        ConfigurableApplicationContext run1 = launch();
        try {
            AccountEngine engine = run1.getBean(AccountEngine.class);

            engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
            buyOrderId = awaitOrderId(engine, "r1");

            engine.publishBuyFill(9001L, buyOrderId, 1L, STOCK, new BigDecimal("10000"), 10);
            awaitHolding(engine, 10);

            AccountState state = engine.accountState(1L);
            balanceAfterFill = state.balance();
            reservedMarginAfterFill = state.reservedMargin();
            unpaidAfterFill = state.unpaid();
            holdingAfterFill = state.holding(STOCK);
        } finally {
            run1.close();
        }

        ConfigurableApplicationContext run2 = launch();
        try {
            AccountEngine engine = run2.getBean(AccountEngine.class);
            AccountState state = engine.accountState(1L);

            assertThat(state.orderIdFor("r1")).as("r1의 orderId가 재시작 뒤에도 재현돼야 한다").isEqualTo(buyOrderId);
            assertThat(state.balance().compareTo(balanceAfterFill)).as("잔고가 복원돼야 한다").isZero();
            assertThat(state.reservedMargin().compareTo(reservedMarginAfterFill)).as("예약증거금이 복원돼야 한다").isZero();
            assertThat(state.unpaid().compareTo(unpaidAfterFill)).as("미수금이 복원돼야 한다").isZero();
            assertThat(state.holding(STOCK)).as("보유 수량이 복원돼야 한다").isEqualTo(holdingAfterFill);

            // 발급기 이월: 새 requestId는 리플레이가 진행시킨 카운터 다음 값을 받아야 한다(재시작 전과 안 겹침).
            engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 5, "r2");
            long newOrderId = awaitOrderId(engine, "r2");
            assertThat(newOrderId).as("새 주문 orderId는 이전 실행의 orderId와 달라야 한다").isNotEqualTo(buyOrderId);
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

    /** requestId에 orderId가 발급될(=accept 처리가 끝날) 때까지 기다린다. */
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
