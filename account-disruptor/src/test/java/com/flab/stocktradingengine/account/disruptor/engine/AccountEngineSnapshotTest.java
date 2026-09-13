package com.flab.stocktradingengine.account.disruptor.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.account.disruptor.domain.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.domain.AccountState;
import com.flab.stocktradingengine.account.disruptor.domain.BuyFillResult;
import com.flab.stocktradingengine.account.disruptor.domain.NoOpAccountResultListener;
import com.flab.stocktradingengine.account.disruptor.domain.RejectReason;
import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.disruptor.io.MatchingOrderSender;
import com.flab.stocktradingengine.account.disruptor.journal.InMemoryAccountJournal;
import com.flab.stocktradingengine.account.disruptor.snapshot.AccountSnapshot;
import com.flab.stocktradingengine.account.disruptor.snapshot.AccountSnapshotCodec;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * 2d-2a — 계좌 엔진 스냅샷 직렬화·복원 라운드트립 검증. 복구 배선(2d-2b, Archive position부터
 * 리플레이)은 아직 안 건드린다 — 여기서는 "엔진A의 계좌 상태를 찍은 스냅샷을 코덱으로 인코딩·
 * 디코딩한 뒤 엔진B에 복원하면 같은 상태가 된다"만 증명한다. matching
 * {@code MatchingEngineSnapshotTest}와 같은 결.
 */
class AccountEngineSnapshotTest {

    private static final long NODE_ID = 0L;
    private static final String STOCK = "005930";
    private static final MatchingOrderSender NO_OP_SENDER = (orderId, accountId, stockCode, side, price, quantity) -> {};

    private final AccountSnapshotCodec codec = new AccountSnapshotCodec();

    private AccountEngine original;
    private AccountEngine restored;

    @AfterEach
    void tearDown() {
        if (original != null) {
            original.shutdown();
        }
        if (restored != null) {
            restored.shutdown();
        }
    }

    @Test
    @DisplayName("계좌 상태를 스냅샷·코덱 왕복 후 복원하면 잔고·예약·보유·미수금·멱등 캐시·발급기 카운터가 원본과 같다")
    void 계좌_상태가_스냅샷_왕복_후_복원된다() throws InterruptedException {
        InMemoryAccountJournal journal = new InMemoryAccountJournal();
        CapturingListener listener = new CapturingListener(2, 1);
        original = new AccountEngine(1024, new BlockingWaitStrategy(), ProducerType.SINGLE, NODE_ID, NO_OP_SENDER, listener, journal);
        original.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"), Map.of(STOCK, 10));
        original.start();

        original.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
        original.publishSell(1L, STOCK, new BigDecimal("10000"), 4, "r2");
        assertTrue(listener.acceptLatch.await(1, TimeUnit.SECONDS), "매수·매도 accept 2건이 도착해야 한다");

        long buyOrderId = listener.orderIds.get("r1");
        original.publishBuyFill(9001L, buyOrderId, 1L, STOCK, new BigDecimal("10000"), 4); // 부분체결 → 예약 잔량 남김
        assertTrue(listener.fillLatch.await(1, TimeUnit.SECONDS), "체결 1건이 도착해야 한다");

        AccountSnapshot snapshot = original.snapshot();
        AccountSnapshot decoded = codec.decode(codec.encode(snapshot));

        restored = new AccountEngine(1024, new BlockingWaitStrategy(), ProducerType.SINGLE, NODE_ID, NO_OP_SENDER, NoOpAccountResultListener.INSTANCE, new InMemoryAccountJournal());
        restored.restore(decoded);
        restored.start();

        AccountState originalState = original.accountState(1L);
        AccountState restoredState = restored.accountState(1L);

        assertEquals(0, originalState.balance().compareTo(restoredState.balance()));
        assertEquals(0, originalState.unpaid().compareTo(restoredState.unpaid()));
        assertEquals(0, originalState.reservedMargin().compareTo(restoredState.reservedMargin()));
        assertEquals(originalState.holding(STOCK), restoredState.holding(STOCK));
        assertEquals(originalState.reservedSellQuantity(STOCK), restoredState.reservedSellQuantity(STOCK));
        assertEquals(buyOrderId, restoredState.orderIdFor("r1"));

        // 발급기 카운터 복원: 복원 뒤 새 requestId는 원본이 이어 발급했을 다음 orderId와 같아야 한다(결정론).
        restored.publishBuy(1L, STOCK, new BigDecimal("10000"), 1, "r3");
        long newOrderId = awaitOrderId(restored, "r3");
        assertEquals(listener.orderIds.get("r2") + 1, newOrderId);
    }

    @Test
    @DisplayName("멱등 캐시(processedTradeIds)도 스냅샷에 담겨 복원된다 — 복원 뒤 같은 tradeId 재도착은 무시된다")
    void 멱등_캐시도_복원된다() throws InterruptedException {
        InMemoryAccountJournal journal = new InMemoryAccountJournal();
        CapturingListener listener = new CapturingListener(1, 1);
        original = new AccountEngine(1024, new BlockingWaitStrategy(), ProducerType.SINGLE, NODE_ID, NO_OP_SENDER, listener, journal);
        original.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        original.start();

        original.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
        assertTrue(listener.acceptLatch.await(1, TimeUnit.SECONDS));
        long orderId = listener.orderIds.get("r1");

        original.publishBuyFill(9001L, orderId, 1L, STOCK, new BigDecimal("10000"), 10);
        assertTrue(listener.fillLatch.await(1, TimeUnit.SECONDS));

        AccountSnapshot decoded = codec.decode(codec.encode(original.snapshot()));

        restored = new AccountEngine(1024, new BlockingWaitStrategy(), ProducerType.SINGLE, NODE_ID, NO_OP_SENDER, NoOpAccountResultListener.INSTANCE, new InMemoryAccountJournal());
        restored.restore(decoded);
        restored.start();

        BuyFillResult replayedFill = restored.accountState(1L).applyBuyFill(9001L, orderId, STOCK, new BigDecimal("10000"), 10);
        assertFalse(replayedFill.applied(), "복원된 상태에도 이미 반영한 tradeId가 멱등 캐시로 남아있어야 한다");
    }

    private long awaitOrderId(AccountEngine engine, String requestId) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        Long orderId;
        while ((orderId = engine.accountState(1L).orderIdFor(requestId)) == null) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("1초 안에 " + requestId + "의 orderId가 발급되지 않음");
            }
            Thread.onSpinWait();
        }
        return orderId;
    }

    /** onAccepted·onSellAccepted에서 requestId→orderId를 기록하고, accept·fill 두 단계로 래치를 내리는 리스너. */
    private static final class CapturingListener implements AccountResultListener {
        private final Map<String, Long> orderIds = new ConcurrentHashMap<>();
        private final CountDownLatch acceptLatch;
        private final CountDownLatch fillLatch;

        CapturingListener(int acceptCount, int fillCount) {
            this.acceptLatch = new CountDownLatch(acceptCount);
            this.fillLatch = new CountDownLatch(fillCount);
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
        }

        @Override
        public void onFillApplied(long accountId, long orderId, long tradeId, boolean applied) {
            fillLatch.countDown();
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
