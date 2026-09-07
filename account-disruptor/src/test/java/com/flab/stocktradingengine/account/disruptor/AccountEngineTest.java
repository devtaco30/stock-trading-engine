package com.flab.stocktradingengine.account.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Disruptor 하네스를 통과하는 매수 검증·예약의 종단 동작.
 *
 * <p>프로듀서(테스트)와 소비자(계좌 핸들러)가 다른 스레드라, 발행 직후 결과가 준비돼 있지 않다.
 * {@link CountDownLatch} 로 기대한 콜백 수가 도착할 때까지 기다린 뒤 검증한다
 * (matching-disruptor 테스트와 동일한 방식).</p>
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
        engine = new AccountEngine(BUFFER_SIZE, new Recorder(events, latch));
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

        engine.publishBuy(1001L, 1L, STOCK, new BigDecimal("10000"), 10, "r1"); // 100000
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

        engine.publishBuy(1001L, 99L, STOCK, new BigDecimal("10000"), 10, "r1");
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

        engine.publishBuy(1001L, 1L, STOCK, new BigDecimal("10000"), 0, "r1");
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

        engine.publishBuy(1001L, 1L, STOCK, new BigDecimal("10000"), 10, "r1"); // 100000 > 75000
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

        engine.publishBuy(1001L, 1L, STOCK, new BigDecimal("60000"), 10, "r1"); // 600000 → 통과
        engine.publishBuy(1002L, 1L, STOCK, new BigDecimal("60000"), 10, "r2"); // 가용 400000 → 거부
        awaitResults();

        assertEquals(2, events.size());
        assertTrue(events.get(0).accepted());
        assertFalse(events.get(1).accepted());
        assertEquals(RejectReason.INSUFFICIENT, events.get(1).reason());
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
            events.add(new Recorded(accountId, orderId, requestId, true, reservedMargin, null));
            latch.countDown();
        }

        @Override
        public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
            events.add(new Recorded(accountId, orderId, requestId, false, null, reason));
            latch.countDown();
        }
    }

    private record Recorded(long accountId, long orderId, String requestId, boolean accepted,
                            BigDecimal reservedMargin, RejectReason reason) {
    }
}
