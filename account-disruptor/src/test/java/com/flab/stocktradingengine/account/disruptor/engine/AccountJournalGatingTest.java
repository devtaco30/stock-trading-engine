package com.flab.stocktradingengine.account.disruptor.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.account.disruptor.domain.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.domain.RejectReason;
import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.disruptor.io.MatchingOrderSender;
import com.flab.stocktradingengine.codec.AccountEventType;
import com.flab.stocktradingengine.codec.AccountJournalEntry;

/**
 * 2b-1 저널 게이팅 검증. matching {@code JournalGatingTest}와 같은 결.
 *
 * <p>{@code handleEventsWith(journal).then(business)} 배선이 실제로 "먼저 기록 → 그다음 반영"
 * 순서를 지키는지, 그리고 매수·매도뿐 아니라 체결·정산까지 전 타입이 저널에 남는지 확인한다.</p>
 *
 * <h3>BUY·SELL 엔트리의 orderId는 0이다</h3>
 * <p>저널러가 비즈니스 핸들러보다 먼저 돌기 때문에, 매수·매도 엔트리를 기록하는 시점엔 orderId가
 * 아직 발급 전이다(발급은 비즈니스 핸들러 안에서 첫 등장 requestId에만 일어난다). 그래서 저널은
 * 그 시점의 입력 그대로(orderId=0)를 담는다 — 리플레이(2b-2)가 그 입력을 같은 결정론적 발급기
 * (2b-0)에 다시 넣어 orderId를 재생성하는 게 설계 의도이지, 저널에 orderId를 미리 못 박아두는
 * 게 아니다. 체결·정산 엔트리의 orderId·tradeId·settlementRef는 계좌가 발급하는 게 아니라
 * 매칭·정산이 넘겨준 입력값이라 저널 시점에 이미 확정돼 있다.</p>
 */
class AccountJournalGatingTest {

    private static final String STOCK = "005930";
    private static final int BUFFER_SIZE = 1024;
    private static final MatchingOrderSender NO_OP_SENDER = (orderId, accountId, stockCode, side, price, quantity) -> {};

    private AccountEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    @Test
    @DisplayName("주문·체결·정산 전 타입이 저널에 처리 순서대로 남는다")
    void 전_타입이_저널에_순서대로_남는다() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(5); // 매수accept(1)+매수거부(1)+체결(1)+정산(1)+매도accept(1)
        engine = new AccountEngine(BUFFER_SIZE, 0L, NO_OP_SENDER, new CountingListener(latch));
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"), Map.of(STOCK, 10));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1"); // accept, orderId=1
        engine.publishBuy(1L, STOCK, new BigDecimal("999999999"), 10, "r2"); // INSUFFICIENT여도 orderId=2로 기록됨
        engine.publishBuyFill(9001L, 1L, 1L, STOCK, new BigDecimal("10000"), 10); // 체결(미수금 생김)
        engine.publishSettlement(7001L, 1L, new BigDecimal("1")); // 직전 체결로 생긴 미수금 안에서 소액 정산
        engine.publishSell(1L, STOCK, new BigDecimal("10000"), 4, "r3"); // accept, orderId=3

        assertTrue(latch.await(1, TimeUnit.SECONDS), "1초 안에 결과 5개가 도착해야 한다");

        List<AccountJournalEntry> entries = engine.journal().entries();
        assertEquals(5, entries.size());
        // BUY·SELL 엔트리의 orderId는 0이다 — 저널러가 비즈니스 핸들러보다 먼저 돌아서, orderId가
        // 아직 발급되기 전(입력 그대로)을 기록하기 때문이다. 리플레이(2b-2)는 이 입력들
        // (accountId·stockCode·price·quantity·requestId)을 그대로 다시 넣어 같은 결정론적
        // 발급기(2b-0)로 orderId를 재생성한다 — 저널에 orderId를 미리 못 박아두지 않는다.
        assertEquals(AccountEventType.BUY, entries.get(0).type());
        assertEquals(0L, entries.get(0).orderId());
        assertEquals(1L, entries.get(0).accountId());
        assertEquals("r1", entries.get(0).requestId());
        assertEquals(AccountEventType.BUY, entries.get(1).type());
        assertEquals(0L, entries.get(1).orderId());
        assertEquals("r2", entries.get(1).requestId());
        // 체결·정산의 orderId·tradeId·settlementRef는 매칭·정산이 넘겨준 입력값이라(계좌가 발급하는
        // 게 아니다) 저널 시점에 이미 확정돼 있다.
        assertEquals(AccountEventType.BUY_FILL, entries.get(2).type());
        assertEquals(1L, entries.get(2).orderId());
        assertEquals(9001L, entries.get(2).tradeId());
        assertEquals(AccountEventType.SETTLEMENT, entries.get(3).type());
        assertEquals(7001L, entries.get(3).tradeId()); // settlementRef가 tradeId 필드 재사용(AccountEvent 참고)
        assertEquals(AccountEventType.SELL, entries.get(4).type());
        assertEquals(0L, entries.get(4).orderId());
        assertEquals("r3", entries.get(4).requestId());
    }

    @Test
    @DisplayName("체결 반영 콜백 시점엔 그 체결이 이미 저널에 있다(게이팅)")
    void 체결_반영_시점엔_이미_저널됨() throws InterruptedException {
        CountDownLatch fillLatch = new CountDownLatch(1);
        AtomicInteger journalSizeAtFill = new AtomicInteger(-1);
        AccountResultListener listener = new NoOpAccountResultListener() {
            @Override
            public void onFillApplied(long accountId, long orderId, long tradeId, boolean applied) {
                // 이 콜백은 비즈니스 핸들러 스레드에서 불린다. 저널러가 비즈니스보다 먼저 돈다면,
                // 이 시점엔 매수+체결 둘 다 이미 저널에 남아 있어야 한다.
                journalSizeAtFill.set(engine.journal().entries().size());
                fillLatch.countDown();
            }
        };
        engine = new AccountEngine(BUFFER_SIZE, 0L, NO_OP_SENDER, listener);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
        engine.publishBuyFill(9001L, 1L, 1L, STOCK, new BigDecimal("10000"), 10);

        assertTrue(fillLatch.await(1, TimeUnit.SECONDS), "1초 안에 체결 콜백이 도착해야 한다");
        assertTrue(journalSizeAtFill.get() >= 2, "체결 반영 시점엔 매수+체결 둘 다 이미 저널에 있어야 한다");
    }

    /** 모든 콜백에서 래치만 내리는 테스트용 리스너. */
    private static final class CountingListener implements AccountResultListener {
        private final CountDownLatch latch;

        CountingListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin) {
            latch.countDown();
        }

        @Override
        public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
            latch.countDown();
        }

        @Override
        public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
            latch.countDown();
        }

        @Override
        public void onFillApplied(long accountId, long orderId, long tradeId, boolean applied) {
            latch.countDown();
        }

        @Override
        public void onSettlementApplied(long accountId, long settlementRef, boolean applied) {
            latch.countDown();
        }

        @Override
        public void onUnpaidRecorded(long accountId, long tradeId, BigDecimal amount) {
        }

        @Override
        public void onDuplicateRequest(long accountId, String requestId) {
        }
    }

    /** 특정 콜백만 오버라이드해 쓰려는 테스트를 위한 전부 no-op 기본 구현. */
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
