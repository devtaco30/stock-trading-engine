package com.flab.stocktradingengine.account.disruptor;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.codec.AccountEventType;
import com.flab.stocktradingengine.codec.AccountJournalEntry;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * 2b-2a — 저널 리플레이 복구({@link AccountEngine#recover}) 검증. Aeron 없이 인메모리 저널만으로,
 * "라이브로 만든 상태 == 저널을 재적용해 복구한 상태"를 증명한다. Archive에서 실제로 읽어오는
 * 배선은 2b-2b(account-worker) 몫이다.
 */
class AccountEngineRecoveryTest {

    private static final long NODE_ID = 0L;
    private static final String STOCK = "005930";
    private static final MatchingOrderSender NO_OP_SENDER = (orderId, accountId, stockCode, side, price, quantity) -> {};

    private AccountEngine original;
    private AccountEngine recovered;

    @AfterEach
    void tearDown() {
        if (original != null) {
            original.shutdown();
        }
        if (recovered != null) {
            recovered.shutdown();
        }
    }

    @Test
    @DisplayName("저널을 재적용하면 잔고·보유·예약증거금·미수금·dedup이 원본과 같아진다")
    void 저널_재적용_상태가_원본과_같다() throws InterruptedException {
        InMemoryAccountJournal journal = new InMemoryAccountJournal();
        OrderIdCapturingListener originalListener = new OrderIdCapturingListener(2, 3);
        original = new AccountEngine(1024, new BlockingWaitStrategy(), ProducerType.SINGLE, NODE_ID, NO_OP_SENDER, originalListener, journal);
        original.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"), Map.of(STOCK, 10));
        original.start();

        original.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
        original.publishSell(1L, STOCK, new BigDecimal("10000"), 4, "r2");
        assertTrue(originalListener.acceptLatch.await(1, TimeUnit.SECONDS), "매수·매도 accept 2건이 도착해야 한다");

        long buyOrderId = originalListener.orderIdFor("r1");
        long sellOrderId = originalListener.orderIdFor("r2");
        original.publishBuyFill(9001L, buyOrderId, 1L, STOCK, new BigDecimal("10000"), 10);
        original.publishSellFill(9002L, sellOrderId, 1L, STOCK, 4);
        original.publishSettlement(7001L, 1L, new BigDecimal("1"));
        assertTrue(originalListener.restLatch.await(1, TimeUnit.SECONDS), "체결·정산 3건이 도착해야 한다");

        AccountState originalState = original.accountState(1L);
        BigDecimal originalBalance = originalState.balance();
        int originalHolding = originalState.holding(STOCK);
        BigDecimal originalReservedMargin = originalState.reservedMargin();
        BigDecimal originalUnpaid = originalState.unpaid();

        List<AccountJournalEntry> entries = journal.entries();

        recovered = new AccountEngine(1024, NODE_ID, NO_OP_SENDER, new NoOpAccountResultListener());
        recovered.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"), Map.of(STOCK, 10));
        recovered.recover(entries);
        recovered.start();

        AccountState recoveredState = recovered.accountState(1L);
        assertEquals(0, originalBalance.compareTo(recoveredState.balance()), "잔고가 원본과 같아야 한다");
        assertEquals(originalHolding, recoveredState.holding(STOCK), "보유 수량이 원본과 같아야 한다");
        assertEquals(0, originalReservedMargin.compareTo(recoveredState.reservedMargin()), "예약증거금이 원본과 같아야 한다");
        assertEquals(0, originalUnpaid.compareTo(recoveredState.unpaid()), "미수금이 원본과 같아야 한다");
        assertEquals(buyOrderId, recoveredState.orderIdFor("r1"), "r1의 orderId가 원본과 같이 재현돼야 한다");
        assertEquals(sellOrderId, recoveredState.orderIdFor("r2"), "r2의 orderId가 원본과 같이 재현돼야 한다");
    }

    @Test
    @DisplayName("재전송하면 리플레이로 재현된 것과 같은 orderId로 중복 통지된다(결정론 발급기 이월)")
    void 재전송하면_같은_orderId로_중복통지() throws InterruptedException {
        InMemoryAccountJournal journal = new InMemoryAccountJournal();
        OrderIdCapturingListener originalListener = new OrderIdCapturingListener(1, 0);
        original = new AccountEngine(1024, new BlockingWaitStrategy(), ProducerType.SINGLE, NODE_ID, NO_OP_SENDER, originalListener, journal);
        original.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        original.start();
        original.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
        assertTrue(originalListener.acceptLatch.await(1, TimeUnit.SECONDS));
        long originalOrderId = originalListener.orderIdFor("r1");

        CountDownLatch dupLatch = new CountDownLatch(1);
        long[] captured = new long[1];
        AccountResultListener listener = new NoOpAccountResultListener() {
            @Override
            public void onDuplicateRequest(long accountId, long orderId, String requestId) {
                captured[0] = orderId;
                dupLatch.countDown();
            }
        };
        recovered = new AccountEngine(1024, NODE_ID, NO_OP_SENDER, listener);
        recovered.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        recovered.recover(journal.entries());
        recovered.start();

        recovered.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
        assertTrue(dupLatch.await(1, TimeUnit.SECONDS), "재전송이 중복 통지로 와야 한다");
        assertEquals(originalOrderId, captured[0], "재전송의 orderId가 원본과 같아야 한다(결정론)");
    }

    @Test
    @DisplayName("복구 뒤 첫 신규 requestId는 리플레이가 진행시킨 카운터 다음 값을 받는다(발급기 이월)")
    void 복구_뒤_신규_주문은_카운터를_이어받는다() throws InterruptedException {
        InMemoryAccountJournal journal = new InMemoryAccountJournal();
        OrderIdCapturingListener originalListener = new OrderIdCapturingListener(2, 0);
        original = new AccountEngine(1024, new BlockingWaitStrategy(), ProducerType.SINGLE, NODE_ID, NO_OP_SENDER, originalListener, journal);
        original.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        original.start();
        original.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
        original.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r2");
        assertTrue(originalListener.acceptLatch.await(1, TimeUnit.SECONDS));
        long lastReplayedOrderId = Math.max(originalListener.orderIdFor("r1"), originalListener.orderIdFor("r2"));

        CountDownLatch newLatch = new CountDownLatch(1);
        OrderIdCapturingListener recoveredListener = new OrderIdCapturingListener(1, 0);
        recovered = new AccountEngine(1024, new BlockingWaitStrategy(), ProducerType.SINGLE, NODE_ID, NO_OP_SENDER, recoveredListener, new InMemoryAccountJournal());
        recovered.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        recovered.recover(journal.entries());
        recovered.start();

        recovered.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r3");
        assertTrue(recoveredListener.acceptLatch.await(1, TimeUnit.SECONDS));
        long newOrderId = recoveredListener.orderIdFor("r3");

        assertNotEquals(originalListener.orderIdFor("r1"), newOrderId);
        assertNotEquals(originalListener.orderIdFor("r2"), newOrderId);
        assertTrue(newOrderId > lastReplayedOrderId, "새 주문 orderId는 리플레이가 진행시킨 카운터 다음 값이어야 한다");
    }

    @Test
    @DisplayName("recover 중에는 엔진에 넘긴 리스너가 한 번도 안 불린다(부수효과 없음)")
    void recover_중엔_리스너가_안_불린다() {
        InMemoryAccountJournal journal = new InMemoryAccountJournal();
        journal.append(new AccountJournalEntry(
            AccountEventType.BUY, 0L, 1L, STOCK, new BigDecimal("10000"), 10, "r1", 0L));

        AtomicInteger callCount = new AtomicInteger(0);
        AccountResultListener countingListener = new NoOpAccountResultListener() {
            @Override
            public void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin) {
                callCount.incrementAndGet();
            }
        };
        recovered = new AccountEngine(1024, NODE_ID, NO_OP_SENDER, countingListener);
        recovered.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        recovered.recover(journal.entries());

        assertEquals(0, callCount.get(), "recover 중엔 엔진 리스너가 호출되면 안 된다");
    }

    @Test
    @DisplayName("빈 저널을 복구하면 시드한 상태 그대로 유지된다")
    void 빈_저널_복구는_아무것도_안_바꾼다() {
        recovered = new AccountEngine(1024, NODE_ID, NO_OP_SENDER, new NoOpAccountResultListener());
        recovered.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"), Map.of(STOCK, 10));
        recovered.recover(List.of());
        recovered.start();

        AccountState state = recovered.accountState(1L);
        assertEquals(0, new BigDecimal("1000000").compareTo(state.balance()));
        assertEquals(10, state.holding(STOCK));
        assertEquals(0, BigDecimal.ZERO.compareTo(state.reservedMargin()));
    }

    @Test
    @DisplayName("저널에 poison 엔트리(예약 없는 체결)가 섞여 있어도 recover가 예외 없이 끝나고 정상 엔트리만 반영된다 (U1)")
    void 저널에_poison_섞여도_recover_예외없이_정상엔트리만_반영() {
        List<AccountJournalEntry> entries = List.of(
            new AccountJournalEntry(AccountEventType.BUY, 0L, 1L, STOCK, new BigDecimal("10000"), 10, "r1", 0L),
            // orderId=999는 매수 접수 저널이 없어 예약이 없다 — recoveryHandler.onEvent에서
            // AccountState.applyBuyFill이 IllegalStateException을 던지는 poison 엔트리.
            new AccountJournalEntry(AccountEventType.BUY_FILL, 999L, 1L, STOCK, new BigDecimal("10000"), 10, null, 9001L)
        );

        recovered = new AccountEngine(1024, NODE_ID, NO_OP_SENDER, new NoOpAccountResultListener());
        recovered.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));

        assertDoesNotThrow(() -> recovered.recover(entries), "poison 엔트리 하나가 recover 전체를 죽여선 안 된다");

        recovered.start();
        AccountState recoveredState = recovered.accountState(1L);
        assertNotEquals(0L, recoveredState.orderIdFor("r1"), "poison 앞의 정상 BUY 엔트리는 반영돼 orderId가 발급돼 있어야 한다");
    }

    /** onAccepted·onSellAccepted에서 requestId→orderId를 기록하고, 지정한 두 단계로 래치를 내리는 리스너. */
    private static final class OrderIdCapturingListener implements AccountResultListener {
        private final Map<String, Long> orderIds = new ConcurrentHashMap<>();
        private final CountDownLatch acceptLatch;
        private final CountDownLatch restLatch;

        OrderIdCapturingListener(int acceptCount, int restCount) {
            this.acceptLatch = new CountDownLatch(acceptCount);
            this.restLatch = new CountDownLatch(restCount);
        }

        long orderIdFor(String requestId) {
            return orderIds.get(requestId);
        }

        @Override
        public void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin) {
            orderIds.put(requestId, orderId);
            acceptLatch.countDown();
        }

        @Override
        public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
            orderIds.put(requestId, orderId);
            acceptLatch.countDown();
        }

        @Override
        public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
            acceptLatch.countDown();
        }

        @Override
        public void onFillApplied(long accountId, long orderId, long tradeId, boolean applied) {
            restLatch.countDown();
        }

        @Override
        public void onSettlementApplied(long accountId, long settlementRef, boolean applied) {
            restLatch.countDown();
        }

        @Override
        public void onUnpaidRecorded(long accountId, long tradeId, BigDecimal amount) {
        }

        @Override
        public void onDuplicateRequest(long accountId, long orderId, String requestId) {
        }
    }

    /** 전부 no-op인 기본 구현 — 필요한 콜백만 오버라이드해 쓴다. */
    private static class NoOpAccountResultListener implements AccountResultListener {
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
        public void onFillApplied(long accountId, long orderId, long tradeId, boolean applied) {
        }

        @Override
        public void onSettlementApplied(long accountId, long settlementRef, boolean applied) {
        }

        @Override
        public void onUnpaidRecorded(long accountId, long tradeId, BigDecimal amount) {
        }

        @Override
        public void onDuplicateRequest(long accountId, long orderId, String requestId) {
        }
    }
}
