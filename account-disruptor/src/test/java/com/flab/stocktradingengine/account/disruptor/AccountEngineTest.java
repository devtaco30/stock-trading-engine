package com.flab.stocktradingengine.account.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * Disruptor 하네스를 통과하는 매수 검증·예약의 종단 동작.
 *
 * <p>프로듀서(테스트)와 소비자(계좌 핸들러)가 다른 스레드라, 발행 직후 결과가 준비돼 있지 않다.
 * {@link CountDownLatch} 로 기대한 콜백 수가 도착할 때까지 기다린 뒤 검증한다
 * (matching-disruptor 테스트와 동일한 방식).</p>
 *
 * <h3>orderId 예측 (C5-2a)</h3>
 * <p>{@link #prepare}가 매번 새 {@link AtomicLong}(0에서 시작하는 {@code incrementAndGet})을
 * orderId 발급 시드로 엔진에 주입한다 — requestId가 비어있지 않고 처음 등장할 때만(계좌·accept·reject
 * 무관) 소비되므로, 테스트가 그 호출 순서만 세면 발급될 orderId를 그대로 예측해 하드코딩할 수 있다.</p>
 */
class AccountEngineTest {

    private static final String STOCK = "005930";
    private static final int BUFFER_SIZE = 1024;

    private final List<Recorded> events = new CopyOnWriteArrayList<>();
    private AccountEngine engine;
    private CountDownLatch latch;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    /** 기대 콜백 수만큼 래치를 걸고 엔진을 만든다. 시드·start 는 테스트가 이어서 한다. */
    private void prepare(int expectedResults) {
        latch = new CountDownLatch(expectedResults);
        engine = new AccountEngine(BUFFER_SIZE, new AtomicLong(0)::incrementAndGet, new Recorder(events, latch));
    }

    private void awaitResults() throws InterruptedException {
        assertTrue(latch.await(1, TimeUnit.SECONDS), "1초 안에 결과가 도착해야 한다");
    }

    @Test
    @DisplayName("시드된 계좌에 충분한 매수가 오면 통과시키고 예약 증거금을 낸다")
    void 충분하면_통과하고_예약증거금() throws InterruptedException {
        prepare(1);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1"); // 100000
        awaitResults();

        assertEquals(1, events.size());
        Recorded event = events.get(0);
        assertTrue(event.accepted());
        assertEquals(0, event.reservedMargin().compareTo(new BigDecimal("40000")));
    }

    @Test
    @DisplayName("워커가 소유하지 않은 계좌면 ACCOUNT_NOT_FOUND 로 거부한다")
    void 모르는_계좌면_거부() throws InterruptedException {
        prepare(1);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuy(99L, STOCK, new BigDecimal("10000"), 10, "r1");
        awaitResults();

        assertEquals(1, events.size());
        assertFalse(events.get(0).accepted());
        assertEquals(RejectReason.ACCOUNT_NOT_FOUND, events.get(0).reason());
    }

    @Test
    @DisplayName("수량이 0 이하면 INVALID_QUANTITY 로 거부한다")
    void 수량이상이면_거부() throws InterruptedException {
        prepare(1);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 0, "r1");
        awaitResults();

        assertEquals(1, events.size());
        assertEquals(RejectReason.INVALID_QUANTITY, events.get(0).reason());
    }

    @Test
    @DisplayName("매수 가능 금액을 넘으면 INSUFFICIENT 로 거부한다")
    void 초과하면_거부() throws InterruptedException {
        prepare(1);
        engine.seed(1L, new BigDecimal("30000"), new BigDecimal("0.40")); // buyLimit = 75000
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1"); // 100000 > 75000
        awaitResults();

        assertEquals(1, events.size());
        assertEquals(RejectReason.INSUFFICIENT, events.get(0).reason());
    }

    @Test
    @DisplayName("같은 계좌 연속 매수는 단일 소비자가 직렬 처리해 둘째가 첫째 예약을 반영한다")
    void 연속매수_러닝예약_직렬처리() throws InterruptedException {
        prepare(2);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("1.00")); // buyLimit = 가용
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("60000"), 10, "r1"); // 600000 → 통과
        engine.publishBuy(1L, STOCK, new BigDecimal("60000"), 10, "r2"); // 가용 400000 → 거부
        awaitResults();

        assertEquals(2, events.size());
        assertTrue(events.get(0).accepted());
        assertFalse(events.get(1).accepted());
        assertEquals(RejectReason.INSUFFICIENT, events.get(1).reason());
    }

    // ---------- requestId 재전송 멱등 하네스 배선 (C5-1a) ----------

    @Test
    @DisplayName("같은 requestId로 매수가 두 번 오면 둘째는 재예약 없이 onDuplicateRequest로 알리고 첫째와 같은 orderId를 돌려준다")
    void 매수_같은requestId_재전송하면_중복통지() throws InterruptedException {
        prepare(2); // 매수 접수(1) + 재전송(1)
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("1.00")); // buyLimit = 가용(재예약됐다면 둘째가 거부됐을 금액)
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("60000"), 10, "r1"); // 600000, 가용 400000 남음
        engine.publishBuy(1L, STOCK, new BigDecimal("60000"), 10, "r1"); // 같은 requestId 재전송(600000 > 400000이라 재예약됐다면 거부)
        awaitResults();

        assertEquals(2, events.size());
        assertTrue(events.get(0).accepted());
        assertTrue(events.get(1).duplicate());
        assertEquals(events.get(0).orderId(), events.get(1).orderId()); // 재전송은 새 orderId를 발급하지 않고 원래 값을 돌려준다(C5-2a)
    }

    @Test
    @DisplayName("같은 requestId로 매도가 두 번 오면 둘째는 재예약 없이 onDuplicateRequest로 알린다")
    void 매도_같은requestId_재전송하면_중복통지() throws InterruptedException {
        prepare(4); // 매수 접수·체결로 보유 확보(2) + 매도 예약(1) + 재전송(1)
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1"); // orderId=1
        engine.publishBuyFill(9001L, 1L, 1L, STOCK, new BigDecimal("10000"), 10); // 보유 10 확보
        engine.publishSell(1L, STOCK, 4, "r2"); // orderId=2
        engine.publishSell(1L, STOCK, 4, "r2"); // 같은 requestId 재전송
        awaitResults();

        assertEquals(4, events.size());
        assertTrue(events.get(2).accepted());
        assertTrue(events.get(3).duplicate());
        assertEquals(events.get(2).orderId(), events.get(3).orderId());
    }

    @Test
    @DisplayName("거부됐던 requestId가 재전송돼도 재처리하지 않고 같은 orderId로 onDuplicateRequest 알린다")
    void 거부된requestId_재전송해도_재처리안함() throws InterruptedException {
        prepare(2); // 거부(1) + 재전송(1)
        engine.seed(1L, new BigDecimal("30000"), new BigDecimal("0.40")); // buyLimit = 75000
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1"); // 100000 > 75000 → 거부(orderId=1 발급·기억은 됨)
        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1"); // 같은 requestId 재전송
        awaitResults();

        assertEquals(2, events.size());
        assertFalse(events.get(0).accepted());
        assertEquals(RejectReason.INSUFFICIENT, events.get(0).reason());
        assertTrue(events.get(1).duplicate());
        assertEquals(events.get(0).orderId(), events.get(1).orderId()); // 거부된 주문도 orderId는 발급·기억되어 재전송 시 같은 값을 돌려준다(C5-2a)
    }

    // ---------- requestId 빈값 가드 (C5-1c) ----------

    @Test
    @DisplayName("빈 requestId 매수는 orderId 미발급(0)으로 INVALID_REQUEST_ID 거부하고, 같은 빈 requestId 둘째도 오염 없이 다시 거부하며, 뒤이은 유효 요청은 시퀀스가 안 밀린 orderId를 받는다")
    void 빈_requestId_매수_INVALID_거부_세트오염없음() throws InterruptedException {
        prepare(3);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "");
        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, ""); // 서로 다른 주문, requestId만 우연히 같은 빈 값
        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r-valid");
        awaitResults();

        assertEquals(3, events.size());
        assertFalse(events.get(0).accepted());
        assertEquals(RejectReason.INVALID_REQUEST_ID, events.get(0).reason());
        assertEquals(0L, events.get(0).orderId()); // 발급 안 됨(NO_ORDER_ID)
        assertFalse(events.get(1).accepted());
        assertEquals(RejectReason.INVALID_REQUEST_ID, events.get(1).reason()); // 재전송(duplicate)이 아니라 또 INVALID_REQUEST_ID여야 오염 없음
        assertEquals(0L, events.get(1).orderId());
        assertTrue(events.get(2).accepted());
        assertEquals(1L, events.get(2).orderId()); // 빈 requestId 둘이 시퀀스를 안 먹었다면 첫 발급값은 1
    }

    @Test
    @DisplayName("null requestId 매수는 orderId 미발급(0)으로 INVALID_REQUEST_ID 로 거부한다")
    void null_requestId_매수는_거부() throws InterruptedException {
        prepare(1);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, null);
        awaitResults();

        assertEquals(1, events.size());
        assertFalse(events.get(0).accepted());
        assertEquals(RejectReason.INVALID_REQUEST_ID, events.get(0).reason());
        assertEquals(0L, events.get(0).orderId());
    }

    @Test
    @DisplayName("빈 requestId 매도는 orderId 미발급(0)으로 INVALID_REQUEST_ID 로 거부한다")
    void 빈_requestId_매도는_거부() throws InterruptedException {
        prepare(1);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishSell(1L, STOCK, 1, "");
        awaitResults();

        assertEquals(1, events.size());
        assertFalse(events.get(0).accepted());
        assertEquals(RejectReason.INVALID_REQUEST_ID, events.get(0).reason());
        assertEquals(0L, events.get(0).orderId());
    }

    // ---------- orderId 발급 (C5-2a) ----------

    @Test
    @DisplayName("매수 첫 접수는 발급된 orderId를 onAccepted로 돌려준다")
    void 매수_첫접수_발급된_orderId_반환() throws InterruptedException {
        prepare(1);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
        awaitResults();

        assertTrue(events.get(0).accepted());
        assertEquals(1L, events.get(0).orderId()); // 이 엔진에서 처음 발급되는 orderId
    }

    // ---------- 체결 반영 하네스 배선 (B3b) ----------

    @Test
    @DisplayName("매수 체결이 하네스를 통과하면 반영하고 onFillApplied(applied=true) 로 알린다")
    void 매수체결_반영하고_통지() throws InterruptedException {
        prepare(2); // 매수 접수(1) + 체결 반영(1)
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1"); // orderId=1
        engine.publishBuyFill(9001L, 1L, 1L, STOCK, new BigDecimal("10000"), 10);
        awaitResults();

        assertEquals(2, events.size());
        Recorded fillEvent = events.get(1);
        assertEquals(9001L, fillEvent.tradeId());
        assertTrue(fillEvent.applied());
    }

    @Test
    @DisplayName("매도 체결이 하네스를 통과하면 반영하고 onFillApplied(applied=true) 로 알린다")
    void 매도체결_반영하고_통지() throws InterruptedException {
        prepare(4); // 매수 접수·체결로 보유 확보(2) + 매도 예약(1) + 매도 체결(1)
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1"); // orderId=1
        engine.publishBuyFill(9001L, 1L, 1L, STOCK, new BigDecimal("10000"), 10); // 보유 10 확보
        engine.publishSell(1L, STOCK, 4, "r2"); // orderId=2
        engine.publishSellFill(9002L, 2L, 1L, STOCK, 4);
        awaitResults();

        assertEquals(4, events.size());
        Recorded fillEvent = events.get(3);
        assertEquals(9002L, fillEvent.tradeId());
        assertTrue(fillEvent.applied());
    }

    // ---------- 매도 보유예약 하네스 배선 ----------

    @Test
    @DisplayName("보유 수량 안이면 매도 검증을 통과시키고 onSellAccepted 로 알린다")
    void 매도예약_보유안이면_통과_통지() throws InterruptedException {
        prepare(3); // 매수 접수·체결로 보유 확보(2) + 매도 예약(1)
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1"); // orderId=1
        engine.publishBuyFill(9001L, 1L, 1L, STOCK, new BigDecimal("10000"), 10); // 보유 10 확보
        engine.publishSell(1L, STOCK, 4, "r2"); // orderId=2
        awaitResults();

        Recorded sellEvent = events.get(2);
        assertTrue(sellEvent.accepted());
        assertEquals(4, sellEvent.reservedQuantity());
    }

    @Test
    @DisplayName("초기 보유를 시드하면 매수 체결 없이도 바로 매도 검증을 통과시킨다")
    void 초기보유_시드하면_바로_매도가능() throws InterruptedException {
        prepare(1);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"), Map.of(STOCK, 10));
        engine.start();

        engine.publishSell(1L, STOCK, 4, "r1");
        awaitResults();

        assertTrue(events.get(0).accepted());
        assertEquals(4, events.get(0).reservedQuantity());
    }

    @Test
    @DisplayName("보유 수량을 넘는 매도는 INSUFFICIENT_HOLDING 으로 거부한다")
    void 매도예약_보유초과하면_거부() throws InterruptedException {
        prepare(1);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40")); // 보유 0
        engine.start();

        engine.publishSell(1L, STOCK, 1, "r1");
        awaitResults();

        assertEquals(1, events.size());
        assertFalse(events.get(0).accepted());
        assertEquals(RejectReason.INSUFFICIENT_HOLDING, events.get(0).reason());
    }

    @Test
    @DisplayName("체결 대상 계좌를 워커가 소유하지 않으면 ACCOUNT_NOT_FOUND 로 거부한다")
    void 체결_모르는계좌면_거부() throws InterruptedException {
        prepare(1);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuyFill(9001L, 1001L, 99L, STOCK, new BigDecimal("10000"), 10);
        awaitResults();

        assertEquals(1, events.size());
        assertFalse(events.get(0).accepted());
        assertEquals(RejectReason.ACCOUNT_NOT_FOUND, events.get(0).reason());
    }

    @Test
    @DisplayName("같은 tradeId 로 매수 체결이 하네스에 두 번 오면 둘째는 applied=false 로 알린다")
    void 매수체결_같은tradeId_둘째는_applied_false() throws InterruptedException {
        prepare(3); // 매수 접수(1) + 체결(1) + 중복 체결(1)
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1"); // orderId=1
        engine.publishBuyFill(9001L, 1L, 1L, STOCK, new BigDecimal("10000"), 10);
        engine.publishBuyFill(9001L, 1L, 1L, STOCK, new BigDecimal("10000"), 10); // 같은 tradeId 재도착
        awaitResults();

        assertEquals(3, events.size());
        assertTrue(events.get(1).applied());
        assertFalse(events.get(2).applied());
    }

    // ---------- 정산 되돌림 하네스 배선 (settlement, a1) ----------

    @Test
    @DisplayName("정산이 하네스를 통과하면 반영하고 onSettlementApplied(applied=true) 로 알린다")
    void 정산_반영하고_통지() throws InterruptedException {
        prepare(3); // 매수 접수(1) + 매수 체결(1) + 정산(1)
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1"); // orderId=1
        engine.publishBuyFill(9001L, 1L, 1L, STOCK, new BigDecimal("10000"), 10); // 미수금 60000 생김
        engine.publishSettlement(7001L, 1L, new BigDecimal("60000"));
        awaitResults();

        assertEquals(3, events.size());
        Recorded settlementEvent = events.get(2);
        assertEquals(7001L, settlementEvent.tradeId());
        assertTrue(settlementEvent.applied());
    }

    // ---------- ProducerType 주입 (order-manager 준비) ----------

    @Test
    @DisplayName("ProducerType.MULTI로 생성하면 두 스레드가 동시에 발행해도 전부 처리된다")
    void 멀티프로듀서_동시발행_전부처리() throws InterruptedException {
        int perThread = 200;
        int total = perThread * 2;
        latch = new CountDownLatch(total);
        engine = new AccountEngine(4096, new BlockingWaitStrategy(), ProducerType.MULTI,
            new AtomicLong(0)::incrementAndGet, new Recorder(events, latch));
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        Thread t1 = new Thread(() -> publishBuys(perThread));
        Thread t2 = new Thread(() -> publishBuys(perThread));
        t1.start();
        t2.start();
        t1.join();
        t2.join();

        assertTrue(latch.await(2, TimeUnit.SECONDS), "2초 안에 결과가 모두 도착해야 한다");
        assertEquals(total, events.size());
    }

    private void publishBuys(int count) {
        for (int i = 0; i < count; i++) {
            engine.publishBuy(1L, STOCK, new BigDecimal("1"), 1, "r"); // 전부 같은 requestId — 첫 건만 accept, 나머지는 duplicate
        }
    }

    /** 소비자 스레드가 낸 결과를 모으고 래치를 내리는 테스트용 리스너. */
    private static final class Recorder implements AccountResultListener {
        private final List<Recorded> events;
        private final CountDownLatch latch;

        Recorder(List<Recorded> events, CountDownLatch latch) {
            this.events = events;
            this.latch = latch;
        }

        @Override
        public void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin) {
            events.add(new Recorded(accountId, orderId, requestId, true, reservedMargin, null, null, null, null, false));
            latch.countDown();
        }

        @Override
        public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
            events.add(new Recorded(accountId, orderId, requestId, true, null, null, null, null, reservedQuantity, false));
            latch.countDown();
        }

        @Override
        public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
            events.add(new Recorded(accountId, orderId, requestId, false, null, reason, null, null, null, false));
            latch.countDown();
        }

        @Override
        public void onFillApplied(long accountId, long orderId, long tradeId, boolean applied) {
            events.add(new Recorded(accountId, orderId, null, false, null, null, tradeId, applied, null, false));
            latch.countDown();
        }

        @Override
        public void onSettlementApplied(long accountId, long settlementRef, boolean applied) {
            events.add(new Recorded(accountId, 0L, null, false, null, null, settlementRef, applied, null, false));
            latch.countDown();
        }

        @Override
        public void onUnpaidRecorded(long accountId, long tradeId, BigDecimal amount) {
        }

        @Override
        public void onDuplicateRequest(long accountId, long orderId, String requestId) {
            events.add(new Recorded(accountId, orderId, requestId, false, null, null, null, null, null, true));
            latch.countDown();
        }
    }

    private record Recorded(long accountId, long orderId, String requestId, boolean accepted,
                            BigDecimal reservedMargin, RejectReason reason,
                            Long tradeId, Boolean applied, Integer reservedQuantity, boolean duplicate) {
    }
}
