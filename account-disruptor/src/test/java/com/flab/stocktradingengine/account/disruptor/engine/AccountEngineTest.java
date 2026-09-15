package com.flab.stocktradingengine.account.disruptor.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import com.flab.stocktradingengine.account.disruptor.domain.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.domain.NoOpAccountResultListener;
import com.flab.stocktradingengine.account.disruptor.domain.RejectReason;
import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.disruptor.engine.AccountOrderIdGenerator;
import com.flab.stocktradingengine.account.disruptor.journal.AccountJournal;
import com.flab.stocktradingengine.codec.AccountJournalEntry;
import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * Disruptor 하네스를 통과하는 매수 검증·예약의 종단 동작.
 *
 * <p>프로듀서(테스트)와 소비자(계좌 핸들러)가 다른 스레드라, 발행 직후 결과가 준비돼 있지 않다.
 * {@link CountDownLatch} 로 기대한 콜백 수가 도착할 때까지 기다린 뒤 검증한다
 * (matching-disruptor 테스트와 동일한 방식).</p>
 *
 * <h3>orderId 예측 (2b-0, 결정론적)</h3>
 * <p>{@link #prepare}가 매번 nodeId=0으로 엔진을 만든다 — {@link AccountOrderIdGenerator}는
 * requestId가 비어있지 않고 처음 등장할 때만(계좌·accept·reject 무관) 카운터를 증가시키므로,
 * 테스트가 그 호출 순서만 세면 발급될 orderId(nodeId=0이라 1,2,3…)를 그대로 예측해 하드코딩할
 * 수 있다.</p>
 */
class AccountEngineTest {

    private static final String STOCK = "005930";
    private static final int BUFFER_SIZE = 1024;
    private static final long NODE_ID = 0L;

    private final List<Recorded> events = new CopyOnWriteArrayList<>();
    private final List<ForwardedOrder> forwardedOrders = new CopyOnWriteArrayList<>();
    private final List<StateChange> stateChanges = new CopyOnWriteArrayList<>();
    private AccountEngine engine;
    private CountDownLatch latch;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    /**
     * 기대 콜백 수만큼 래치를 걸고 엔진을 만든다. 시드·start 는 테스트가 이어서 한다.
     * matchingOrderSender는 accept마다 {@link #forwardedOrders}에 기록하는 캡처용을 항상 심어둔다(②-b) —
     * 대부분 테스트는 그 리스트를 안 보고, 발신 자체를 검증하는 테스트만 확인한다.
     */
    private void prepare(int expectedResults) {
        latch = new CountDownLatch(expectedResults);
        engine = new AccountEngine(BUFFER_SIZE, NODE_ID,
            (orderId, accountId, stockCode, side, price, quantity) ->
                forwardedOrders.add(new ForwardedOrder(orderId, accountId, stockCode, side, price, quantity)),
            new Recorder(events, stateChanges, latch));
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
    @DisplayName("같은 requestId로 매수가 두 번 오면 둘째는 재예약 없이 onDuplicateRequest로만 알린다")
    void 매수_같은requestId_재전송하면_중복통지() throws InterruptedException {
        prepare(2); // 매수 접수(1) + 재전송(1)
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("1.00")); // buyLimit = 가용(재예약됐다면 둘째가 거부됐을 금액)
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("60000"), 10, "r1"); // 600000, 가용 400000 남음
        engine.publishBuy(1L, STOCK, new BigDecimal("60000"), 10, "r1"); // 같은 requestId 재전송(600000 > 400000이라 재예약됐다면 거부)
        awaitResults();

        assertEquals(2, events.size());
        assertTrue(events.get(0).accepted());
        // 재예약됐다면 가용 초과로 거부(accepted=false)가 나왔을 것 — duplicate로만 통지됐다는 게
        // 재예약을 안 했다는 증거다.
        assertTrue(events.get(1).duplicate());
    }

    @Test
    @DisplayName("같은 requestId로 매도가 두 번 오면 둘째는 재예약 없이 onDuplicateRequest로 알린다")
    void 매도_같은requestId_재전송하면_중복통지() throws InterruptedException {
        prepare(4); // 매수 접수·체결로 보유 확보(2) + 매도 예약(1) + 재전송(1)
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1"); // orderId=1
        engine.publishBuyFill(9001L, 1L, 1L, STOCK, new BigDecimal("10000"), 10); // 보유 10 확보
        engine.publishSell(1L, STOCK, new BigDecimal("10000"), 4, "r2"); // orderId=2
        engine.publishSell(1L, STOCK, new BigDecimal("10000"), 4, "r2"); // 같은 requestId 재전송
        awaitResults();

        assertEquals(4, events.size());
        assertTrue(events.get(2).accepted());
        assertTrue(events.get(3).duplicate());
    }

    @Test
    @DisplayName("거부됐던 requestId가 재전송돼도 재처리하지 않고 onDuplicateRequest로만 알린다")
    void 거부된requestId_재전송해도_재처리안함() throws InterruptedException {
        prepare(2); // 거부(1) + 재전송(1)
        engine.seed(1L, new BigDecimal("30000"), new BigDecimal("0.40")); // buyLimit = 75000
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1"); // 100000 > 75000 → 거부(requestId는 기억됨)
        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1"); // 같은 requestId 재전송
        awaitResults();

        assertEquals(2, events.size());
        assertFalse(events.get(0).accepted());
        assertEquals(RejectReason.INSUFFICIENT, events.get(0).reason());
        // 재전송이 다시 검증됐다면 또 INSUFFICIENT 거부가 나왔을 것 — duplicate로만 통지됐다는 게
        // 재처리를 안 했다는 증거다.
        assertTrue(events.get(1).duplicate());
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

        engine.publishSell(1L, STOCK, new BigDecimal("10000"), 1, "");
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

    // ---------- 결정론적 orderId — 리플레이 전제 (2b-0) ----------

    /**
     * 리플레이(2b)가 성립하려면 "같은 입력이면 같은 orderId"가 보장돼야 한다 — 그래야 리플레이로
     * 재현한 계좌 상태가 이미 매칭에 실어 보낸 orderId와 어긋나지 않는다. 이 테스트가 그 전제를
     * 직접 증명한다: 완전히 새로운 엔진 두 개(같은 nodeId)에 같은 입력 시퀀스를 넣고, 발급된
     * orderId가 완전히 동일한지 본다 — 거부·중복·매도까지 섞어 "발급 자체는 accept·reject와
     * 무관하게 첫 등장에서만" 규칙이 카운터 결정론을 안 깨는지도 같이 확인한다.
     */
    @Test
    @DisplayName("같은 nodeId·같은 입력 시퀀스를 다른 엔진 두 개에 넣으면 발급되는 orderId가 완전히 같다(2b-0, 리플레이 전제)")
    void 같은_입력이면_다른_엔진에서도_같은_orderId_발급() throws InterruptedException {
        List<Long> firstRun = runDeterminismSequence();
        List<Long> secondRun = runDeterminismSequence();

        assertEquals(firstRun, secondRun);
    }

    /**
     * 매수 accept(1) → 매수 거부(2, INSUFFICIENT — 발급은 되지만 거부) → 같은 requestId 재전송(3,
     * 새 orderId 없이 기존값 반환) → 매도 accept(4)까지 섞은 시퀀스를 한 엔진에 넣고, 발급된
     * orderId를 발생 순서대로 모아 돌려준다. 호출마다 엔진을 새로 만들어 fresh 상태에서 돈다.
     */
    private List<Long> runDeterminismSequence() throws InterruptedException {
        prepare(4);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"), Map.of(STOCK, 100));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1"); // orderId=1, accept
        engine.publishBuy(1L, STOCK, new BigDecimal("999999999"), 10, "r2"); // orderId=2, INSUFFICIENT(거부여도 발급은 됨)
        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1"); // 재전송 — 새 orderId 없이 1 반환
        engine.publishSell(1L, STOCK, new BigDecimal("10000"), 5, "r3"); // orderId=3, accept
        awaitResults();

        List<Long> orderIds = events.stream().map(Recorded::orderId).toList();
        engine.shutdown();
        events.clear();
        forwardedOrders.clear();
        return orderIds;
    }

    // ---------- 매칭으로 발신 (②-b) ----------

    @Test
    @DisplayName("매수가 accept되면 매칭으로 forwardPlace(BUY)가 올바른 필드로 딱 한 번 불린다")
    void 매수_accept되면_매칭발신() throws InterruptedException {
        prepare(1);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
        awaitResults();

        assertTrue(events.get(0).accepted());
        assertEquals(1, forwardedOrders.size());
        ForwardedOrder forwarded = forwardedOrders.get(0);
        assertEquals(events.get(0).orderId(), forwarded.orderId());
        assertEquals(1L, forwarded.accountId());
        assertEquals(STOCK, forwarded.stockCode());
        assertEquals(OrderSide.BUY, forwarded.side());
        assertEquals(0, new BigDecimal("10000").compareTo(forwarded.price()));
        assertEquals(10, forwarded.quantity());
    }

    @Test
    @DisplayName("매도가 accept되면 매칭으로 forwardPlace(SELL)가 올바른 필드로 딱 한 번 불린다")
    void 매도_accept되면_매칭발신() throws InterruptedException {
        prepare(1);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"), Map.of(STOCK, 10));
        engine.start();

        engine.publishSell(1L, STOCK, new BigDecimal("20000"), 4, "r1");
        awaitResults();

        assertTrue(events.get(0).accepted());
        assertEquals(1, forwardedOrders.size());
        ForwardedOrder forwarded = forwardedOrders.get(0);
        assertEquals(events.get(0).orderId(), forwarded.orderId());
        assertEquals(1L, forwarded.accountId());
        assertEquals(STOCK, forwarded.stockCode());
        assertEquals(OrderSide.SELL, forwarded.side());
        assertEquals(0, new BigDecimal("20000").compareTo(forwarded.price()));
        assertEquals(4, forwarded.quantity());
    }

    @Test
    @DisplayName("거부·중복·invalid는 매칭으로 발신하지 않는다")
    void 거부_중복_invalid는_매칭발신안함() throws InterruptedException {
        prepare(4); // 초과거부(1) + 재전송중복(1) + invalid quantity(1) + 모르는계좌(1)
        engine.seed(1L, new BigDecimal("30000"), new BigDecimal("0.40")); // buyLimit = 75000
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1"); // 100000 > 75000 → INSUFFICIENT
        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1"); // 재전송 → duplicate
        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 0, "r2"); // INVALID_QUANTITY
        engine.publishBuy(99L, STOCK, new BigDecimal("10000"), 10, "r3"); // ACCOUNT_NOT_FOUND
        awaitResults();

        assertEquals(4, events.size());
        assertFalse(events.get(0).accepted());
        assertTrue(events.get(1).duplicate());
        assertFalse(events.get(2).accepted());
        assertFalse(events.get(3).accepted());
        assertTrue(forwardedOrders.isEmpty());
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
        engine.publishSell(1L, STOCK, new BigDecimal("10000"), 4, "r2"); // orderId=2
        engine.publishSellFill(9002L, 2L, 1L, STOCK, 4);
        awaitResults();

        assertEquals(4, events.size());
        Recorded fillEvent = events.get(3);
        assertEquals(9002L, fillEvent.tradeId());
        assertTrue(fillEvent.applied());
    }

    // ---------- 상태변경 통지 (계좌 상태 영속/프로젝션 트랙 Unit 2) ----------

    @Test
    @DisplayName("매수 체결이 반영되면 onStateChanged가 그 시점 잔고·보유·seq로 호출된다")
    void 매수체결_반영되면_상태변경_통지() throws InterruptedException {
        prepare(2); // 매수 접수(1) + 체결 반영(1)
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1"); // orderId=1
        engine.publishBuyFill(9001L, 1L, 1L, STOCK, new BigDecimal("10000"), 10);
        awaitResults();

        assertEquals(1, stateChanges.size(), "체결 1건이 상태변경 통지도 1건 내야 한다");
        StateChange change = stateChanges.get(0);
        assertEquals(1L, change.accountId());
        assertEquals(0, change.balance().compareTo(engine.accountState(1L).balance()));
        assertEquals(Map.of(STOCK, 10), change.holdings());
        assertEquals(engine.accountState(1L).seq(), change.seq());
    }

    @Test
    @DisplayName("매도 체결이 반영되면 onStateChanged가 그 시점 보유로 호출된다")
    void 매도체결_반영되면_상태변경_통지() throws InterruptedException {
        prepare(4); // 매수 접수·체결로 보유 확보(2) + 매도 예약(1) + 매도 체결(1)
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
        engine.publishBuyFill(9001L, 1L, 1L, STOCK, new BigDecimal("10000"), 10); // 보유 10
        engine.publishSell(1L, STOCK, new BigDecimal("10000"), 4, "r2");
        engine.publishSellFill(9002L, 2L, 1L, STOCK, 4); // 보유 6
        awaitResults();

        assertEquals(2, stateChanges.size(), "체결 2건(매수·매도)이 상태변경 통지도 2건 내야 한다");
        StateChange lastChange = stateChanges.get(1);
        assertEquals(Map.of(STOCK, 6), lastChange.holdings());
    }

    @Test
    @DisplayName("정산이 반영되면 onStateChanged가 그 시점 잔고로 호출된다")
    void 정산_반영되면_상태변경_통지() throws InterruptedException {
        prepare(3); // 매수 접수(1) + 매수 체결(1) + 정산(1)
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
        engine.publishBuyFill(9001L, 1L, 1L, STOCK, new BigDecimal("10000"), 10); // 미수금 60000 생김
        engine.publishSettlement(7001L, 1L, new BigDecimal("60000"));
        awaitResults();

        assertEquals(2, stateChanges.size(), "체결 1건 + 정산 1건 = 상태변경 통지 2건");
        StateChange settlementChange = stateChanges.get(1);
        assertEquals(0, settlementChange.balance().compareTo(engine.accountState(1L).balance()));
    }

    @Test
    @DisplayName("예약 accept·거부·재전송(멱등 무시)은 잔고·보유가 안 바뀌므로 onStateChanged를 호출하지 않는다")
    void 예약accept_거부_재전송은_상태변경_통지_안함() throws InterruptedException {
        prepare(4); // 매수 접수(1, accept) + 매수 접수2(1, 거부) + 재전송(1, duplicate) + 체결(1)
        engine.seed(1L, new BigDecimal("30000"), new BigDecimal("0.40")); // buyLimit=75000
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 5, "r1"); // 50000 <= 75000 → accept
        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r2"); // 100000 > 75000 → 거부
        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 5, "r1"); // 재전송 → duplicate
        engine.publishBuyFill(9001L, 1L, 1L, STOCK, new BigDecimal("10000"), 5); // 정상 체결(상태변경 1건)
        awaitResults();

        assertEquals(1, stateChanges.size(), "accept·거부·재전송은 상태변경 없이 체결 1건만 통지돼야 한다");
    }

    // ---------- content-poison 방어: 도메인 예외 격리 (U1) ----------

    @Test
    @DisplayName("예약 없는 주문에 대한 체결(도메인 예외)이 와도 이벤트 하나만 폐기하고 다음 이벤트는 정상 처리한다")
    void 예약없는체결은_폐기하고_다음이벤트는_정상처리() throws InterruptedException {
        prepare(1); // poison(publishBuyFill)은 onFillApplied 호출 전에 예외가 나 콜백이 없다 — 뒤이은 매수 accept 1건만 기대
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        // orderId=999는 매수 접수를 한 적이 없어 예약이 없다 — AccountState.applyBuyFill이
        // IllegalStateException을 던지는 지점(도메인 불변식 위반, 매칭이 검증 안 된 주문을 체결시킨 셈).
        engine.publishBuyFill(9001L, 999L, 1L, STOCK, new BigDecimal("10000"), 10);
        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
        awaitResults();

        assertEquals(1, events.size());
        assertTrue(events.get(0).accepted());
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
        engine.publishSell(1L, STOCK, new BigDecimal("10000"), 4, "r2"); // orderId=2
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

        engine.publishSell(1L, STOCK, new BigDecimal("10000"), 4, "r1");
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

        engine.publishSell(1L, STOCK, new BigDecimal("10000"), 1, "r1");
        awaitResults();

        assertEquals(1, events.size());
        assertFalse(events.get(0).accepted());
        assertEquals(RejectReason.INSUFFICIENT_HOLDING, events.get(0).reason());
    }

    @Test
    @DisplayName("매도 price가 없거나 0 이하면 INVALID_QUANTITY 로 거부한다(②-a, 매수와 대칭)")
    void 매도_price가_잘못되면_거부() throws InterruptedException {
        prepare(2);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"), Map.of(STOCK, 10));
        engine.start();

        engine.publishSell(1L, STOCK, null, 4, "r1");
        engine.publishSell(1L, STOCK, new BigDecimal("0"), 4, "r2");
        awaitResults();

        assertEquals(2, events.size());
        assertFalse(events.get(0).accepted());
        assertEquals(RejectReason.INVALID_QUANTITY, events.get(0).reason());
        assertFalse(events.get(1).accepted());
        assertEquals(RejectReason.INVALID_QUANTITY, events.get(1).reason());
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
            NODE_ID,
            (orderId, accountId, stockCode, side, price, quantity) ->
                forwardedOrders.add(new ForwardedOrder(orderId, accountId, stockCode, side, price, quantity)),
            new Recorder(events, stateChanges, latch));
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

    // ---------- shutdown 안전성 (죽은 소비자 앞 무한 대기 방지) ----------

    /**
     * 저널 소비자(1단계)가 죽으면, 저널 소비자 자신은 Disruptor의 hasBacklog 검사에서
     * 빠진다(BatchEventProcessor.run()의 finally가 running을 IDLE로 되돌려 "죽은 소비자"가
     * "안 도는 소비자"와 구분이 안 됨) — 그래서 뒤에 물린 비즈니스 핸들러(체인의 마지막,
     * hasBacklog가 실제로 보는 대상)가 죽은 저널 소비자의 시퀀스를 영원히 기다리며 블록된
     * 채(죽지 않고 running=true) 멈춰야 shutdown()이 실제로 무한 대기한다. 비즈니스 핸들러
     * 자신이 죽는 시나리오로는 이 hang이 재현되지 않는다(그 핸들러도 죽으면 검사 대상에서
     * 빠져 shutdown()이 바로 반환됨 — Disruptor 4.0.0 소스로 확인).
     */
    @Test
    @DisplayName("저널 소비자가 죽어 뒤 핸들러가 영원히 블록돼도 shutdown()이 타임아웃 안에 반환한다")
    void 저널소비자가_죽어_핸들러가_블록돼도_shutdown은_타임아웃_안에_반환() throws InterruptedException {
        CountDownLatch journalReached = new CountDownLatch(1);
        CountDownLatch shutdownReturned = new CountDownLatch(1);

        AccountEngine deadJournalEngine = new AccountEngine(BUFFER_SIZE, new BlockingWaitStrategy(), ProducerType.SINGLE,
            NODE_ID,
            (orderId, accountId, stockCode, side, price, quantity) -> { },
            NoOpAccountResultListener.INSTANCE,
            new PoisonAccountJournal(journalReached));
        deadJournalEngine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        deadJournalEngine.start();

        deadJournalEngine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
        assertTrue(journalReached.await(1, TimeUnit.SECONDS), "저널 소비자가 append에 도달해야 한다");

        Thread shutdownThread = new Thread(() -> {
            deadJournalEngine.shutdown();
            shutdownReturned.countDown();
        });
        shutdownThread.setDaemon(true);
        shutdownThread.start();

        assertTrue(shutdownReturned.await(8, TimeUnit.SECONDS), "8초 안에 shutdown()이 반환해야 한다");
    }

    /** 소비자 스레드가 낸 결과를 모으고 래치를 내리는 테스트용 리스너. */
    private static final class Recorder implements AccountResultListener {
        private final List<Recorded> events;
        private final List<StateChange> stateChanges;
        private final CountDownLatch latch;

        Recorder(List<Recorded> events, List<StateChange> stateChanges, CountDownLatch latch) {
            this.events = events;
            this.stateChanges = stateChanges;
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
        public void onDuplicateRequest(long accountId, String requestId) {
            events.add(new Recorded(accountId, 0L, requestId, false, null, null, null, null, null, true));
            latch.countDown();
        }

        /**
         * 공유 latch(결과 콜백 개수 기준)와 별도로 카운트한다 — onFillApplied·onSettlementApplied보다
         * 먼저 불리므로(AccountEventHandler 호출 순서), latch가 풀리는 시점엔 이미 기록돼 있다.
         */
        @Override
        public void onStateChanged(long accountId, BigDecimal balance, Map<String, Integer> holdings, long seq) {
            stateChanges.add(new StateChange(accountId, balance, holdings, seq));
        }
    }

    private record Recorded(long accountId, long orderId, String requestId, boolean accepted,
                            BigDecimal reservedMargin, RejectReason reason,
                            Long tradeId, Boolean applied, Integer reservedQuantity, boolean duplicate) {
    }

    /** onStateChanged 호출 한 건의 캡처(계좌 상태 영속/프로젝션 트랙 Unit 2). */
    private record StateChange(long accountId, BigDecimal balance, Map<String, Integer> holdings, long seq) {
    }

    /** matchingOrderSender.forwardPlace 호출 한 건의 캡처(②-b). */
    private record ForwardedOrder(long orderId, long accountId, String stockCode, OrderSide side, BigDecimal price, int quantity) {
    }

    /** append 호출 시 latch를 내리고 예외를 던져 저널 소비자를 죽이는 테스트용 저널(shutdown 안전성 검증). */
    private static final class PoisonAccountJournal implements AccountJournal {
        private final CountDownLatch reached;

        PoisonAccountJournal(CountDownLatch reached) {
            this.reached = reached;
        }

        @Override
        public void append(AccountJournalEntry entry) {
            reached.countDown();
            throw new RuntimeException("의도적 저널 소비자 사망(테스트)");
        }

        @Override
        public List<AccountJournalEntry> entries() {
            return List.of();
        }

        @Override
        public long position() {
            return 0L;
        }
    }
}
