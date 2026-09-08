package com.flab.stocktradingengine.account.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import com.flab.stocktradingengine.account.disruptor.AccountEngine;
import com.flab.stocktradingengine.account.disruptor.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.RejectReason;
import com.flab.stocktradingengine.kafka.event.TradeFilledEvent;

/**
 * account-fills 토픽에서 받은 {@link TradeFilledEvent} 하나를 {@link AccountEngine}의
 * 매수·매도 양쪽에 전부 반영하는지 검증한다(fan-out 설계: 같은 이벤트가 accountId 키만
 * 다르게 두 번 발행되지만, 소비자는 매번 양쪽을 다 부르고 소유하지 않은 쪽은
 * ACCOUNT_NOT_FOUND 로 엔진이 알아서 무시한다).
 */
class AccountFillConsumerTest {

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

    private void prepare(int expectedResults) {
        latch = new CountDownLatch(expectedResults);
        engine = new AccountEngine(BUFFER_SIZE, new Recorder(events, latch));
    }

    private void awaitResults() throws InterruptedException {
        assertTrue(latch.await(1, TimeUnit.SECONDS), "1초 안에 결과가 도착해야 한다");
    }

    @Test
    @DisplayName("체결 이벤트 하나를 받으면 매수·매도 양쪽 예약에 전부 반영하고 커밋을 확인한다")
    void 체결이벤트_양쪽반영_ack확인() throws InterruptedException {
        prepare(4); // 매수 예약(1) + 매도 예약(1) + 매수 체결반영(1) + 매도 체결반영(1)
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40")); // 매수 계좌
        engine.seed(2L, new BigDecimal("1000000"), new BigDecimal("0.40"), Map.of(STOCK, 10)); // 매도 계좌(보유 10)
        engine.start();

        AccountFillConsumer consumer = new AccountFillConsumer(engine);
        Acknowledgment ack = mock(Acknowledgment.class);
        TradeFilledEvent fill = new TradeFilledEvent(9001L, STOCK, 1001L, 1L, 2001L, 2L, 4, new BigDecimal("10000"));

        engine.publishBuy(1001L, 1L, STOCK, new BigDecimal("10000"), 4, "r1");
        engine.publishSell(2001L, 2L, STOCK, 4, "r2");
        consumer.consume(fill, ack); // 매수·매도 예약이 처리될 스레드가 같아 순서가 보장된다
        awaitResults();

        assertEquals(4, events.size());
        Recorded buyFillEvent = events.get(2);
        Recorded sellFillEvent = events.get(3);
        assertEquals(9001L, buyFillEvent.tradeId());
        assertTrue(buyFillEvent.success());
        assertEquals(9001L, sellFillEvent.tradeId());
        assertTrue(sellFillEvent.success());
        verify(ack).acknowledge();
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
            events.add(new Recorded(accountId, orderId, true, null, null));
            latch.countDown();
        }

        @Override
        public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
            events.add(new Recorded(accountId, orderId, true, null, null));
            latch.countDown();
        }

        @Override
        public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
            events.add(new Recorded(accountId, orderId, false, null, reason));
            latch.countDown();
        }

        @Override
        public void onFillApplied(long accountId, long orderId, long tradeId, boolean applied) {
            events.add(new Recorded(accountId, orderId, applied, tradeId, null));
            latch.countDown();
        }
    }

    private record Recorded(long accountId, long orderId, boolean success, Long tradeId, RejectReason reason) {
    }
}
