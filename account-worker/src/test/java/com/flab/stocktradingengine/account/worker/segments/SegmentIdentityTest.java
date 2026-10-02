package com.flab.stocktradingengine.account.worker.segments;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.lmax.disruptor.BlockingWaitStrategy;

import com.flab.stocktradingengine.account.disruptor.domain.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.domain.RejectReason;
import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.disruptor.io.MatchingOrderSender;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.io.MatchListener;
import com.flab.stocktradingengine.matching.disruptor.journal.InMemoryJournal;
import com.flab.stocktradingengine.time.EpochNanos;
import com.flab.stocktradingengine.time.StageLatencyRegistry;
import com.flab.stocktradingengine.time.StageSnapshot;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.matching.FillResult;

/**
 * 계좌 엔진과 매칭 엔진을 한 프로세스에서 이어(Aeron·Kafka 없이) 주문 한 쌍이 접수, 체결, 계좌 반영까지
 * 흐르는 동안 단계 값이 모두 채워지는지, 같은 체결 한 건의 항등식
 * F = A@aggr + B@aggr + C@aggr + D + E 가 평균으로 성립하는지(잔차 1% 이내, 설계서 4-4절 4번) 확인한다.
 * 프로세스 사이 전달(Aeron)은 이 테스트에서 빠져 있고, 같은 시각 값이 그대로 건네진다고 가정한다.
 */
class SegmentIdentityTest {

    private static final String STOCK = "005930";
    private static final int PAIRS = 20;

    private AccountEngine account;
    private MatchingEngine matching;

    @AfterEach
    void tearDown() {
        if (account != null) {
            account.shutdown();
        }
        if (matching != null) {
            matching.shutdown();
        }
    }

    private static double mean(Map<String, StageSnapshot> snapshots, String stage) {
        return snapshots.get(stage).meanMicros();
    }

    @Test
    void 주문_쌍이_접수_체결_반영까지_흐르면_단계가_채워지고_F_항등식_잔차가_1퍼센트_이내다() throws InterruptedException {
        StageLatencyRegistry accountRegistry = new StageLatencyRegistry(true);
        StageLatencyRegistry matchingRegistry = new StageLatencyRegistry(true);
        AtomicInteger forwarded = new AtomicInteger();
        AtomicInteger applied = new AtomicInteger();
        AtomicLong tradeIds = new AtomicLong(9000);

        matching = new MatchingEngine(1024, new BlockingWaitStrategy(), new MatchListener() {
            @Override
            public void onFill(String stockCode, FillResult fill) {
                throw new AssertionError("계측이 켜져 있으면 시각을 실은 호출이 와야 한다");
            }

            @Override
            public void onFill(String stockCode, FillResult fill, long emittedAtNanos, long aggressorOriginNanos) {
                long tradeId = tradeIds.incrementAndGet();
                long receivedAtNanos = EpochNanos.now();
                account.publishBuyFill(tradeId, fill.buyOrderId(), fill.buyAccountId(), stockCode, fill.matchPrice(),
                    fill.filledQuantity(), 0L, 0, receivedAtNanos, emittedAtNanos, aggressorOriginNanos);
                account.publishSellFill(tradeId, fill.sellOrderId(), fill.sellAccountId(), stockCode,
                    fill.filledQuantity(), 0L, 0, receivedAtNanos, emittedAtNanos, aggressorOriginNanos);
            }
        }, new InMemoryJournal(), matchingRegistry);

        MatchingOrderSender toMatching = new MatchingOrderSender() {
            @Override
            public void forwardPlace(long orderId, long accountId, String stockCode, OrderSide side, BigDecimal price, int quantity) {
                throw new AssertionError("계측이 켜져 있으면 시각을 실은 호출이 와야 한다");
            }

            @Override
            public void forwardPlace(long orderId, long accountId, String stockCode, OrderSide side, BigDecimal price, int quantity,
                                     long originNanos, long receivedAtNanos, long acceptedAtNanos) {
                matching.publishPlace(orderId, accountId, stockCode, side, price, quantity, Instant.now(), 0L, 0,
                    originNanos, receivedAtNanos, acceptedAtNanos, EpochNanos.now());
                forwarded.incrementAndGet();
            }
        };

        AccountResultListener listener = new AccountResultListener() {
            @Override
            public void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin) {
            }

            @Override
            public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
            }

            @Override
            public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
            }

            @Override
            public void onFillApplied(long accountId, long orderId, long tradeId, boolean wasApplied) {
                applied.incrementAndGet();
            }

            @Override
            public void onSettlementApplied(long accountId, long settlementRef, boolean wasApplied) {
            }

            @Override
            public void onUnpaidRecorded(long accountId, long tradeId, BigDecimal amount) {
            }

            @Override
            public void onDuplicateRequest(long accountId, String requestId) {
            }
        };
        account = new AccountEngine(1024, 0L, toMatching, listener, accountRegistry);
        account.seed(1L, new BigDecimal("100000000"), new BigDecimal("0.40"));
        account.seed(2L, new BigDecimal("100000000"), new BigDecimal("0.40"), Map.of(STOCK, 1000));
        matching.start();
        account.start();

        for (int i = 0; i < PAIRS; i++) {
            account.publishSell(2L, STOCK, new BigDecimal("10000"), 1, "s" + i, EpochNanos.now(), EpochNanos.now());
            awaitAtLeast(forwarded, 2 * i + 1);
            account.publishBuy(1L, STOCK, new BigDecimal("10000"), 1, "b" + i, EpochNanos.now(), EpochNanos.now());
            awaitAtLeast(forwarded, 2 * i + 2);
            awaitAtLeast(applied, 2 * (i + 1));
        }

        Map<String, StageSnapshot> accountStages = accountRegistry.snapshotAndResetAll();
        Map<String, StageSnapshot> matchingStages = matchingRegistry.snapshotAndResetAll();

        for (String stage : new String[] {"A", "B", "B_biz", "ring_publish", "hop_to_journal", "journal_append", "hop_to_business"}) {
            assertThat(accountStages.get(stage).count()).as(stage).isEqualTo(2L * PAIRS);
        }
        for (String stage : new String[] {"E", "F", "fill_transit", "fill_hop_to_journal", "fill_journal_append", "fill_hop_to_business"}) {
            assertThat(accountStages.get(stage).count()).as(stage).isEqualTo(PAIRS);
        }
        assertThat(matchingStages.get("C").count()).isEqualTo(2L * PAIRS);
        for (String stage : new String[] {"D", "A@aggr", "B@aggr", "C@aggr"}) {
            assertThat(matchingStages.get(stage).count()).as(stage).isEqualTo(PAIRS);
        }

        double meanF = mean(accountStages, "F");
        double sumOfParts = mean(matchingStages, "A@aggr") + mean(matchingStages, "B@aggr") + mean(matchingStages, "C@aggr")
            + mean(matchingStages, "D") + mean(accountStages, "E");
        double residual = Math.abs(meanF - sumOfParts) / meanF;
        assertThat(residual).as("잔차 비율(mean F=%s, 부분 합=%s)", meanF, sumOfParts).isLessThanOrEqualTo(0.01);
    }

    private static void awaitAtLeast(AtomicInteger counter, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (counter.get() < expected && System.nanoTime() < deadline) {
            Thread.sleep(2);
        }
        assertThat(counter.get()).isGreaterThanOrEqualTo(expected);
    }
}
