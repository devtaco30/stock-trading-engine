package com.flab.stocktradingengine.account.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import com.flab.stocktradingengine.account.disruptor.domain.AccountState;
import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.worker.AccountWorkerApplication;
import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.codec.FillCodec;
import com.flab.stocktradingengine.codec.FilledTrade;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;

/**
 * fork3, Unit 4(fork3 마지막 유닛) — 한 체결의 매수·매도 계좌가 서로 다른 샤드에 있을 때, U2
 * fan-out이 같은 체결을 두 계좌 워커로 보내고 각 샤드가 자기 소유 계좌만 반영하는지(수신 모델 A)
 * 실제 udp 전송으로 end-to-end 확인한다. ipc는 드라이버 로컬이라 크로스컨텍스트가 안 돼 udp가
 * 필수다 — 이 레포 첫 udp 기반 account 테스트이자 첫 REMOTE-over-udp 녹화 검증이다(U3에서는
 * REMOTE-over-ipc만 확인했다).
 *
 * <h3>왜 matching-worker를 안 띄우는가</h3>
 * <p>account-worker 모듈은 matching-worker의 {@code AccountFillPublisher}를 import할 수 없다
 * (모듈 경계). fan-out 계산(라우팅→목적지) 자체는 U2 {@code AccountFillPublisherTest}에서 이미
 * 단위검증됐다 — 이 테스트는 그 fan-out이 만드는 결과(같은 체결이 두 endpoint로 각각 도착)를
 * 테스트가 직접 흉내내({@link AccountFillIntegrationTest}·{@link AccountFillReplayRecoveryIntegrationTest}와
 * 같은 관례) **수신 2샤드 + 소유 필터**만 검증한다.</p>
 */
class CrossShardFillFanoutIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;
    private static final long BUY_ACCOUNT_ID = 501L;
    private static final long SELL_ACCOUNT_ID = 502L;
    private static final int TRADE_QUANTITY = 4;
    private static final BigDecimal TRADE_PRICE = new BigDecimal("10000");
    private static final BigDecimal SEED_BALANCE = new BigDecimal("1000000");
    private static final BigDecimal MARGIN_RATE = new BigDecimal("0.40");
    private static final int SEEDED_HOLDING = 10;

    // 결정적 기준값 — seed 잔고 1,000,000 · margin-rate 0.40, 4주 @ 10,000(=40,000).
    // 예약증거금 40,000*0.40=16,000 → 지급되어 잔고 -16,000, 나머지 24,000은 미수금.
    private static final BigDecimal EXPECTED_BUY_BALANCE = new BigDecimal("984000");
    private static final BigDecimal EXPECTED_UNPAID = new BigDecimal("24000");

    private final FillCodec codec = new FillCodec();

    @TempDir
    private Path archiveDirA;

    @TempDir
    private Path archiveDirB;

    @Test
    void 매수_매도가_서로_다른_샤드일_때_각자_자기_소유_계좌만_반영한다() throws IOException {
        int fillPortA = freePort();
        int fillPortB = freePort();
        int controlPortA = freePort();
        int controlPortB = freePort();

        ConfigurableApplicationContext shardA = launch(fillPortA, controlPortA, archiveDirA,
            "account-worker.seed-accounts[0].account-id=" + BUY_ACCOUNT_ID,
            "account-worker.seed-accounts[0].balance=" + SEED_BALANCE,
            "account-worker.seed-accounts[0].margin-rate=" + MARGIN_RATE);
        ConfigurableApplicationContext shardB = launch(fillPortB, controlPortB, archiveDirB,
            "account-worker.seed-accounts[0].account-id=" + SELL_ACCOUNT_ID,
            "account-worker.seed-accounts[0].balance=" + SEED_BALANCE,
            "account-worker.seed-accounts[0].margin-rate=" + MARGIN_RATE,
            "account-worker.seed-accounts[0].holdings[" + STOCK + "]=" + SEEDED_HOLDING);
        try {
            MediaDriver matchingDriver = MediaDriver.launchEmbedded();
            try {
                Aeron matchingAeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(matchingDriver.aeronDirectoryName()));
                try {
                    AccountEngine engineA = shardA.getBean(AccountEngine.class);
                    AccountEngine engineB = shardB.getBean(AccountEngine.class);

                    engineA.publishBuy(BUY_ACCOUNT_ID, STOCK, TRADE_PRICE, TRADE_QUANTITY, "rb");
                    long buyOrderId = awaitOrderId(engineA, BUY_ACCOUNT_ID, "rb");
                    engineB.publishSell(SELL_ACCOUNT_ID, STOCK, TRADE_PRICE, TRADE_QUANTITY, "rs");
                    long sellOrderId = awaitOrderId(engineB, SELL_ACCOUNT_ID, "rs");

                    Publication pubA = matchingAeron.addPublication(
                        "aeron:udp?endpoint=localhost:" + fillPortA, AeronStreamIds.FILL);
                    Publication pubB = matchingAeron.addPublication(
                        "aeron:udp?endpoint=localhost:" + fillPortB, AeronStreamIds.FILL);
                    try {
                        awaitConnected(pubA);
                        awaitConnected(pubB);

                        // U2 fan-out이 크로스샤드 체결에 내놓는 결과와 동일 — 같은 FilledTrade를
                        // 매수 샤드(A)·매도 샤드(B) 두 목적지에 각각 발행한다.
                        FilledTrade trade = new FilledTrade(
                            9001L, STOCK, buyOrderId, BUY_ACCOUNT_ID, sellOrderId, SELL_ACCOUNT_ID, TRADE_QUANTITY, TRADE_PRICE);
                        send(pubA, trade);
                        send(pubB, trade);

                        awaitHolding(engineA, BUY_ACCOUNT_ID, TRADE_QUANTITY);
                        awaitHolding(engineB, SELL_ACCOUNT_ID, SEEDED_HOLDING - TRADE_QUANTITY);

                        AccountState buyState = engineA.accountState(BUY_ACCOUNT_ID);
                        assertThat(buyState.balance().compareTo(EXPECTED_BUY_BALANCE)).as("매수 체결이 반영된 잔고여야 한다").isZero();
                        assertThat(buyState.reservedMargin().compareTo(BigDecimal.ZERO)).as("예약증거금이 풀려야 한다").isZero();
                        assertThat(buyState.unpaid().compareTo(EXPECTED_UNPAID)).as("매수 체결이 반영된 미수금이어야 한다").isZero();
                        assertThat(engineA.accountState(SELL_ACCOUNT_ID)).as("샤드 A는 매도 계좌를 소유하지 않는다").isNull();

                        AccountState sellState = engineB.accountState(SELL_ACCOUNT_ID);
                        assertThat(sellState.holding(STOCK)).as("매도 체결만큼 보유가 줄어야 한다").isEqualTo(SEEDED_HOLDING - TRADE_QUANTITY);
                        assertThat(engineB.accountState(BUY_ACCOUNT_ID)).as("샤드 B는 매수 계좌를 소유하지 않는다").isNull();
                    } finally {
                        pubA.close();
                        pubB.close();
                    }
                } finally {
                    matchingAeron.close();
                }
            } finally {
                matchingDriver.close();
            }
        } finally {
            shardA.close();
            shardB.close();
        }
    }

    private ConfigurableApplicationContext launch(int fillPort, int controlPort, Path archiveDir, String... seedProperties) {
        List<String> properties = new ArrayList<>(List.of(
            "transport.fill.channel=aeron:udp?endpoint=localhost:" + fillPort,
            "account.worker.archive.control-channel=aeron:udp?endpoint=localhost:" + controlPort,
            "account.worker.archive-dir=" + archiveDir.toAbsolutePath()));
        properties.addAll(List.of(seedProperties));
        return new SpringApplicationBuilder(AccountWorkerApplication.class)
            .web(WebApplicationType.NONE)
            .properties(properties.toArray(new String[0]))
            .run();
    }

    /** OS가 배정하는 빈 UDP 포트를 하나 잡아 즉시 반환한다(TOCTOU 여지는 있으나 로컬 단독 테스트라 리스크가 낮다). */
    private int freePort() throws IOException {
        try (DatagramSocket socket = new DatagramSocket(0)) {
            return socket.getLocalPort();
        }
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

    /** requestId에 orderId가 발급될(=accept 처리가 끝날) 때까지 기다린 뒤 그 orderId를 돌려준다. */
    private long awaitOrderId(AccountEngine engine, long accountId, String requestId) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        Long orderId;
        while ((orderId = engine.accountState(accountId).orderIdFor(requestId)) == null) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 " + requestId + "의 orderId가 발급되지 않음");
            }
            Thread.yield();
        }
        return orderId;
    }

    /** 보유 수량이 기대치에 도달할(=체결 반영이 끝날) 때까지 기다린다. */
    private void awaitHolding(AccountEngine engine, long accountId, int expected) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (engine.accountState(accountId).holding(STOCK) != expected) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 계좌 " + accountId + "의 보유 수량이 " + expected + "에 도달하지 않음");
            }
            Thread.yield();
        }
    }
}
