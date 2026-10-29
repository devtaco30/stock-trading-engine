package com.flab.stocktradingengine.account.disruptor.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.account.disruptor.domain.AccountState;
import com.flab.stocktradingengine.account.disruptor.domain.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.domain.RejectReason;
import com.flab.stocktradingengine.account.disruptor.io.MatchingOrderSender;
import com.flab.stocktradingengine.account.disruptor.journal.InMemoryAccountJournal;
import com.flab.stocktradingengine.codec.AccountEventType;
import com.flab.stocktradingengine.codec.AccountJournalEntry;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * U9e — 매칭행 보관 목록이 가득 찬 종목의 주문을 예약 전에 거부하고, 그 판정을 저널에 실어 복구가
 * 같은 결과를 내는지 검증한다. 판정을 핸들러가 발신기의 순간 상태로 내리면 복구(발신기 no-op)에서는
 * 같은 주문이 접수돼 매칭으로 간 적 없는 예약이 남는다.
 */
class AccountEngineHoldingFullTest {

    private static final long NODE_ID = 0L;
    private static final String STOCK = "005930";

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
    @DisplayName("보관 목록이 가득 차면 매수는 MATCHING_HOLDING_FULL로 거부되고 예약도 매칭 전달도 없다")
    void 가득_차면_거부되고_예약이_없다() throws InterruptedException {
        RejectRecorder listener = new RejectRecorder(1);
        FakeSender sender = new FakeSender(false);
        live = new AccountEngine(1024, new BlockingWaitStrategy(), ProducerType.SINGLE, NODE_ID, sender, listener, new InMemoryAccountJournal());
        live.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        live.start();

        live.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");

        assertTrue(listener.latch.await(1, TimeUnit.SECONDS));
        assertEquals(List.of(RejectReason.MATCHING_HOLDING_FULL), listener.reasons);
        AccountState state = live.accountState(1L);
        assertEquals(0, BigDecimal.ZERO.compareTo(state.reservedMargin()));
        assertEquals(0, sender.forwardCount.get());
    }

    @Test
    @DisplayName("매도도 가득 차면 거부되고 보유 예약이 없다")
    void 가득_차면_매도도_거부된다() throws InterruptedException {
        RejectRecorder listener = new RejectRecorder(1);
        live = new AccountEngine(1024, new BlockingWaitStrategy(), ProducerType.SINGLE, NODE_ID, new FakeSender(false), listener, new InMemoryAccountJournal());
        live.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"), java.util.Map.of(STOCK, 10));
        live.start();

        live.publishSell(1L, STOCK, new BigDecimal("10000"), 4, "r1");

        assertTrue(listener.latch.await(1, TimeUnit.SECONDS));
        assertEquals(List.of(RejectReason.MATCHING_HOLDING_FULL), listener.reasons);
    }

    @Test
    @DisplayName("같은 저널을 발신기가 no-op인 엔진에서 복구해도 같은 거부가 되풀이되고 예약이 없다")
    void 복구해도_같은_거부가_되풀이된다() throws InterruptedException {
        InMemoryAccountJournal journal = new InMemoryAccountJournal();
        RejectRecorder liveListener = new RejectRecorder(1);
        live = new AccountEngine(1024, new BlockingWaitStrategy(), ProducerType.SINGLE, NODE_ID, new FakeSender(false), liveListener, journal);
        live.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        live.start();
        live.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
        assertTrue(liveListener.latch.await(1, TimeUnit.SECONDS));

        recovered = new AccountEngine(1024, NODE_ID, new FakeSender(true), new NoOpAccountResultListener());
        recovered.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        recovered.recover(journal.entries());
        recovered.start();

        AccountState state = recovered.accountState(1L);
        assertEquals(0, BigDecimal.ZERO.compareTo(state.reservedMargin()), "복구가 접수로 바꿔 예약을 만들면 안 된다");
    }

    @Test
    @DisplayName("표시가 없는 옛 저널 엔트리를 복구하면 기존대로 접수되어 예약이 잡힌다")
    void 표시_없는_옛_저널은_기존_동작() {
        List<AccountJournalEntry> oldEntries = List.of(
            new AccountJournalEntry(AccountEventType.BUY, 0L, 1L, STOCK, new BigDecimal("10000"), 10, "r1", 0L));

        recovered = new AccountEngine(1024, NODE_ID, new FakeSender(true), new NoOpAccountResultListener());
        recovered.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        recovered.recover(oldEntries);
        recovered.start();

        assertTrue(recovered.accountState(1L).reservedMargin().signum() > 0);
    }

    @Test
    @DisplayName("보관 여유가 있으면 기존대로 접수되고 매칭으로 전달된다")
    void 여유가_있으면_기존_동작() throws InterruptedException {
        RejectRecorder listener = new RejectRecorder(0);
        FakeSender sender = new FakeSender(true);
        live = new AccountEngine(1024, new BlockingWaitStrategy(), ProducerType.SINGLE, NODE_ID, sender, listener, new InMemoryAccountJournal());
        live.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        live.start();

        live.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");

        assertTrue(sender.forwarded.await(1, TimeUnit.SECONDS));
        assertTrue(listener.reasons.isEmpty());
        assertTrue(live.accountState(1L).reservedMargin().signum() > 0);
    }

    private static final class FakeSender implements MatchingOrderSender {
        private final boolean room;
        private final AtomicInteger forwardCount = new AtomicInteger();
        private final CountDownLatch forwarded = new CountDownLatch(1);

        FakeSender(boolean room) {
            this.room = room;
        }

        @Override
        public void forwardPlace(long orderId, long accountId, String stockCode, OrderSide side, BigDecimal price, int quantity) {
            forwardCount.incrementAndGet();
            forwarded.countDown();
        }

        @Override
        public boolean hasHoldingRoom(String stockCode) {
            return room;
        }
    }

    private static final class RejectRecorder extends NoOpAccountResultListener {
        private final List<RejectReason> reasons = new CopyOnWriteArrayList<>();
        private final CountDownLatch latch;

        RejectRecorder(int expected) {
            this.latch = new CountDownLatch(expected);
        }

        @Override
        public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
            reasons.add(reason);
            latch.countDown();
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
        public void onDuplicateRequest(long accountId, String requestId) {
        }
    }
}
