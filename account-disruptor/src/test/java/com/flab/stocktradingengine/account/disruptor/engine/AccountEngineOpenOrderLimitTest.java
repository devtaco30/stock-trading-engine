package com.flab.stocktradingengine.account.disruptor.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.account.disruptor.domain.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.domain.RejectReason;
import com.flab.stocktradingengine.account.disruptor.io.MatchingOrderSender;
import com.flab.stocktradingengine.account.disruptor.journal.InMemoryAccountJournal;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * 계좌 미체결 상한 — 매수 예약과 매도 예약을 합친 계좌의 미체결 주문 수가 상한에 이르면 새 주문을
 * OPEN_ORDER_LIMIT_EXCEEDED로 거부하고, 기존 주문은 그대로 두며, 체결로 줄면 다시 접수한다.
 * 상한을 지정하지 않은 엔진은 제한 없이 접수한다(기존 동작).
 */
class AccountEngineOpenOrderLimitTest {

    private static final long NODE_ID = 0L;
    private static final String STOCK = "005930";
    private static final BigDecimal PRICE = new BigDecimal("10000");

    private AccountEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    @Test
    @DisplayName("상한까지는 접수되고, 상한에 이른 뒤의 매수는 거부되며 예약도 매칭 전달도 없다")
    void 상한에_이르면_매수가_거부된다() throws InterruptedException {
        Recorder listener = new Recorder(3);
        CountingSender sender = new CountingSender();
        engine = start(2, listener, sender, Map.of());

        engine.publishBuy(1L, STOCK, PRICE, 1, "r1");
        engine.publishBuy(1L, STOCK, PRICE, 1, "r2");
        engine.publishBuy(1L, STOCK, PRICE, 1, "r3");

        assertTrue(listener.latch.await(2, TimeUnit.SECONDS));
        assertEquals(List.of("A:r1", "A:r2", "R:r3:OPEN_ORDER_LIMIT_EXCEEDED"), listener.events);
        assertEquals(2, sender.forwardCount.get());
        assertEquals(2, engine.accountState(1L).openOrderCount());
    }

    @Test
    @DisplayName("상한은 매수 예약과 매도 예약을 합친 개수로 센다")
    void 매수와_매도를_합쳐_센다() throws InterruptedException {
        Recorder listener = new Recorder(4);
        engine = start(2, listener, new CountingSender(), Map.of(STOCK, 10));

        engine.publishBuy(1L, STOCK, PRICE, 1, "b1");
        engine.publishSell(1L, STOCK, PRICE, 1, "s1");
        engine.publishSell(1L, STOCK, PRICE, 1, "s2");
        engine.publishBuy(1L, STOCK, PRICE, 1, "b2");

        assertTrue(listener.latch.await(2, TimeUnit.SECONDS));
        assertEquals(List.of("A:b1", "A:s1", "R:s2:OPEN_ORDER_LIMIT_EXCEEDED", "R:b2:OPEN_ORDER_LIMIT_EXCEEDED"), listener.events);
    }

    @Test
    @DisplayName("체결로 미체결 주문이 줄면 다시 접수한다")
    void 체결로_줄면_다시_접수한다() throws InterruptedException {
        Recorder listener = new Recorder(4);
        engine = start(2, listener, new CountingSender(), Map.of());

        engine.publishBuy(1L, STOCK, PRICE, 1, "r1"); // orderId 1
        engine.publishBuy(1L, STOCK, PRICE, 1, "r2"); // orderId 2
        engine.publishBuy(1L, STOCK, PRICE, 1, "r3"); // 거부, orderId 3
        engine.publishBuyFill(9001L, orderId(1), 1L, STOCK, PRICE, 1, 100L); // r1 전량 체결
        engine.publishBuy(1L, STOCK, PRICE, 1, "r4");

        assertTrue(listener.latch.await(2, TimeUnit.SECONDS));
        assertEquals("A:r4", listener.events.get(listener.events.size() - 1));
    }

    @Test
    @DisplayName("부분체결된 주문은 아직 미체결이라 상한에 계속 센다")
    void 부분체결은_계속_센다() throws InterruptedException {
        Recorder listener = new Recorder(2);
        engine = start(1, listener, new CountingSender(), Map.of());

        engine.publishBuy(1L, STOCK, PRICE, 10, "r1");
        engine.publishBuyFill(9001L, orderId(1), 1L, STOCK, PRICE, 4, 100L); // 4/10 체결
        engine.publishBuy(1L, STOCK, PRICE, 1, "r2");

        assertTrue(listener.latch.await(2, TimeUnit.SECONDS));
        assertEquals(List.of("A:r1", "R:r2:OPEN_ORDER_LIMIT_EXCEEDED"), listener.events);
    }

    @Test
    @DisplayName("한도로 거부된 requestId를 같은 번호로 다시 보내면 중복으로 처리된다")
    void 거부된_requestId_재시도는_중복이다() throws InterruptedException {
        Recorder listener = new Recorder(3);
        engine = start(1, listener, new CountingSender(), Map.of());

        engine.publishBuy(1L, STOCK, PRICE, 1, "r1");
        engine.publishBuy(1L, STOCK, PRICE, 1, "r2"); // 거부
        engine.publishBuy(1L, STOCK, PRICE, 1, "r2"); // 같은 번호 재시도

        assertTrue(listener.latch.await(2, TimeUnit.SECONDS));
        assertEquals(List.of("A:r1", "R:r2:OPEN_ORDER_LIMIT_EXCEEDED", "D:r2"), listener.events);
    }

    @Test
    @DisplayName("상한을 지정하지 않으면 개수와 무관하게 접수한다")
    void 상한이_없으면_제한이_없다() throws InterruptedException {
        Recorder listener = new Recorder(5);
        engine = new AccountEngine(1024, new BlockingWaitStrategy(), ProducerType.SINGLE, NODE_ID, new CountingSender(), listener, new InMemoryAccountJournal());
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        for (int i = 1; i <= 5; i++) {
            engine.publishBuy(1L, STOCK, PRICE, 1, "r" + i);
        }

        assertTrue(listener.latch.await(2, TimeUnit.SECONDS));
        assertEquals(List.of("A:r1", "A:r2", "A:r3", "A:r4", "A:r5"), listener.events);
    }

    @Test
    @DisplayName("상한은 1 이상이어야 하고 엔진 시작 뒤에는 바꿀 수 없다")
    void 상한_검증() {
        engine = new AccountEngine(1024, new BlockingWaitStrategy(), ProducerType.SINGLE, NODE_ID, new CountingSender(), new Recorder(0), new InMemoryAccountJournal());

        assertThrows(IllegalArgumentException.class, () -> engine.setOpenOrderLimit(0));
        assertThrows(IllegalArgumentException.class, () -> engine.setOpenOrderLimit(-3));

        engine.start();
        assertThrows(IllegalStateException.class, () -> engine.setOpenOrderLimit(5));
    }

    private AccountEngine start(int limit, Recorder listener, CountingSender sender, Map<String, Integer> holdings) {
        AccountEngine started = new AccountEngine(1024, new BlockingWaitStrategy(), ProducerType.SINGLE, NODE_ID, sender, listener, new InMemoryAccountJournal());
        started.setOpenOrderLimit(limit);
        started.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"), holdings);
        started.start();
        return started;
    }

    /** AccountOrderIdGenerator의 결정론 규칙((nodeId&lt;&lt;53)|counter, nodeId=0)으로 orderId를 예측한다. 거부된 주문도 orderId를 소비한다. */
    private static long orderId(long counter) {
        return (NODE_ID << 53) | counter;
    }

    private static final class CountingSender implements MatchingOrderSender {
        private final AtomicInteger forwardCount = new AtomicInteger();

        @Override
        public void forwardPlace(long orderId, long accountId, String stockCode, OrderSide side, BigDecimal price, int quantity) {
            forwardCount.incrementAndGet();
        }
    }

    /** 콜백을 "A:requestId", "R:requestId:사유", "D:requestId" 문자열로 순서대로 모은다. */
    private static final class Recorder implements AccountResultListener {
        private final List<String> events = new CopyOnWriteArrayList<>();
        private final CountDownLatch latch;

        Recorder(int expected) {
            this.latch = new CountDownLatch(expected);
        }

        private void add(String event) {
            events.add(event);
            latch.countDown();
        }

        @Override
        public void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin) {
            add("A:" + requestId);
        }

        @Override
        public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
            add("A:" + requestId);
        }

        @Override
        public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
            add("R:" + requestId + ":" + reason);
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
            add("D:" + requestId);
        }
    }
}
