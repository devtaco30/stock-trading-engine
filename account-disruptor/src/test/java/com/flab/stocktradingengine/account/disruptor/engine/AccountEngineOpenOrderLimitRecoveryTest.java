package com.flab.stocktradingengine.account.disruptor.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.account.disruptor.domain.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.domain.RejectReason;
import com.flab.stocktradingengine.account.disruptor.io.MatchingOrderSender;
import com.flab.stocktradingengine.account.disruptor.journal.InMemoryAccountJournal;
import com.flab.stocktradingengine.codec.AccountJournalEntry;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * 계좌 미체결 상한의 복구 — 저널에는 상한 값이 실리지 않고 복구는 엔진 설정값으로 판정한다(저널 형식 불변).
 * 그래서 같은 설정값으로 복구하면 원래 접수·거부가 그대로 재현되고, 설정값이 다르면 재현되지 않는다.
 * 뒤의 두 시험은 「상한을 바꾸려면 저널과 스냅샷을 비우고 새로 시작한다」는 운영 규칙의 근거를 고정한다.
 */
class AccountEngineOpenOrderLimitRecoveryTest {

    private static final long NODE_ID = 0L;
    private static final String STOCK = "005930";
    private static final BigDecimal PRICE = new BigDecimal("10000");

    private AccountEngine live;
    private AccountEngine recovered;

    @AfterEach
    void tearDown() {
        if (live != null) {
            live.shutdown();
        }
        if (recovered != null) {
            recovered.shutdown();
        }
    }

    @Test
    @DisplayName("같은 상한 값으로 복구하면 원래 접수된 주문 수와 같고 거부된 주문은 예약이 없다")
    void 같은_상한으로_복구하면_판정이_같다() throws InterruptedException {
        InMemoryAccountJournal journal = journalOfThreeBuysUnderLimit(2);

        recovered = recoverWith(2, journal.entries());

        assertEquals(2, recovered.accountState(1L).openOrderCount());
    }

    @Test
    @DisplayName("더 큰 상한 값으로 복구하면 원래 거부된 주문이 접수되어 판정이 달라진다")
    void 상한을_올려_복구하면_거부가_접수로_바뀐다() throws InterruptedException {
        InMemoryAccountJournal journal = journalOfThreeBuysUnderLimit(2);

        recovered = recoverWith(3, journal.entries());

        assertEquals(3, recovered.accountState(1L).openOrderCount());
    }

    @Test
    @DisplayName("더 작은 상한 값으로 복구하면 원래 접수된 주문이 거부되어 판정이 달라진다")
    void 상한을_낮춰_복구하면_접수가_거부로_바뀐다() throws InterruptedException {
        InMemoryAccountJournal journal = journalOfThreeBuysUnderLimit(2);

        recovered = recoverWith(1, journal.entries());

        assertEquals(1, recovered.accountState(1L).openOrderCount());
    }

    @Test
    @DisplayName("상한이 없던 시절의 저널을 상한을 두고 복구하면 원래 접수된 주문이 거부된다")
    void 상한_없던_저널을_상한으로_복구하면_판정이_달라진다() throws InterruptedException {
        InMemoryAccountJournal journal = journalOfThreeBuysUnderLimit(0); // 0 = 상한을 설정하지 않은 엔진

        recovered = recoverWith(2, journal.entries());

        assertEquals(2, recovered.accountState(1L).openOrderCount());
    }

    @Test
    @DisplayName("매도도 같은 상한 값으로 복구하면 원래 접수된 주문 수와 같다")
    void 매도도_같은_상한으로_복구하면_판정이_같다() throws InterruptedException {
        InMemoryAccountJournal journal = journalOfThreeSellsUnderLimit(2);

        recovered = recoverSellsWith(2, journal.entries());

        assertEquals(2, recovered.accountState(1L).openOrderCount());
    }

    @Test
    @DisplayName("매도를 더 작은 상한 값으로 복구하면 원래 접수된 주문이 거부되어 판정이 달라진다")
    void 매도를_상한을_낮춰_복구하면_접수가_거부로_바뀐다() throws InterruptedException {
        InMemoryAccountJournal journal = journalOfThreeSellsUnderLimit(2);

        recovered = recoverSellsWith(1, journal.entries());

        assertEquals(1, recovered.accountState(1L).openOrderCount());
    }

    /** limit이 0이면 상한을 설정하지 않은 엔진으로 세 주문을 접수시키고 그 저널을 돌려준다. */
    private InMemoryAccountJournal journalOfThreeBuysUnderLimit(int limit) throws InterruptedException {
        InMemoryAccountJournal journal = new InMemoryAccountJournal();
        CountDownLatch done = new CountDownLatch(3);
        live = new AccountEngine(1024, new BlockingWaitStrategy(), ProducerType.SINGLE, NODE_ID, new NoopSender(), new CountingListener(done), journal);
        if (limit > 0) {
            live.setOpenOrderLimit(limit);
        }
        live.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        live.start();
        live.publishBuy(1L, STOCK, PRICE, 1, "r1");
        live.publishBuy(1L, STOCK, PRICE, 1, "r2");
        live.publishBuy(1L, STOCK, PRICE, 1, "r3");
        assertTrue(done.await(2, TimeUnit.SECONDS));
        return journal;
    }

    /** 보유 10주를 시드한 계좌에서 매도 세 건을 접수시키고(상한 limit) 그 저널을 돌려준다. */
    private InMemoryAccountJournal journalOfThreeSellsUnderLimit(int limit) throws InterruptedException {
        InMemoryAccountJournal journal = new InMemoryAccountJournal();
        CountDownLatch done = new CountDownLatch(3);
        live = new AccountEngine(1024, new BlockingWaitStrategy(), ProducerType.SINGLE, NODE_ID, new NoopSender(), new CountingListener(done), journal);
        live.setOpenOrderLimit(limit);
        live.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"), Map.of(STOCK, 10));
        live.start();
        live.publishSell(1L, STOCK, PRICE, 1, "s1");
        live.publishSell(1L, STOCK, PRICE, 1, "s2");
        live.publishSell(1L, STOCK, PRICE, 1, "s3");
        assertTrue(done.await(2, TimeUnit.SECONDS));
        return journal;
    }

    private AccountEngine recoverSellsWith(int limit, Iterable<AccountJournalEntry> entries) {
        AccountEngine engine = new AccountEngine(1024, NODE_ID, new NoopSender(), new CountingListener(new CountDownLatch(0)));
        engine.setOpenOrderLimit(limit);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"), Map.of(STOCK, 10));
        engine.recover(entries);
        return engine;
    }

    private AccountEngine recoverWith(int limit, Iterable<AccountJournalEntry> entries) {
        AccountEngine engine = new AccountEngine(1024, NODE_ID, new NoopSender(), new CountingListener(new CountDownLatch(0)));
        engine.setOpenOrderLimit(limit);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.recover(entries);
        return engine;
    }

    private static final class NoopSender implements MatchingOrderSender {
        @Override
        public void forwardPlace(long orderId, long accountId, String stockCode, OrderSide side, BigDecimal price, int quantity) {
        }
    }

    private static final class CountingListener implements AccountResultListener {
        private final CountDownLatch done;

        CountingListener(CountDownLatch done) {
            this.done = done;
        }

        @Override
        public void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin) {
            done.countDown();
        }

        @Override
        public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
            done.countDown();
        }

        @Override
        public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
            done.countDown();
        }

        @Override
        public void onFillApplied(long accountId, long orderId, long tradeId, boolean applied) {
        }

        @Override
        public void onSettlementApplied(long accountId, long settlementRef, boolean applied) {
        }

        @Override
        public void onUnpaidRecorded(long accountId, long tradeId, BigDecimal amount) {
        }

        @Override
        public void onDuplicateRequest(long accountId, String requestId) {
            done.countDown();
        }
    }
}
