package com.flab.stocktradingengine.account.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import com.flab.stocktradingengine.account.disruptor.domain.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.domain.RejectReason;
import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.worker.messaging.AccountSettlementConsumer;
import com.flab.stocktradingengine.kafka.event.SettlementResultEvent;

/**
 * account-settlements 토픽에서 받은 {@link SettlementResultEvent} 하나를
 * {@link AccountEngine#publishSettlement}로 그대로 전달하는지 검증한다.
 */
class AccountSettlementConsumerTest {

    private static final int BUFFER_SIZE = 1024;

    private AccountEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    @Test
    @DisplayName("정산 이벤트를 받으면 반영하고 커밋을 확인한다")
    void 정산이벤트_반영하고_ack확인() throws InterruptedException {
        List<Recorded> events = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(3); // 매수 접수(1) + 매수 체결(1, 미수금 60000 생김) + 정산 반영(1)
        engine = new AccountEngine(BUFFER_SIZE, 0L, (orderId, accountId, stockCode, side, price, quantity) -> {}, new Recorder(events, latch));
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();
        engine.publishBuy(1L, "005930", new BigDecimal("10000"), 10, "r1"); // orderId=1
        engine.publishBuyFill(9001L, 1L, 1L, "005930", new BigDecimal("10000"), 10);

        AccountSettlementConsumer consumer = new AccountSettlementConsumer(engine);
        Acknowledgment ack = mock(Acknowledgment.class);
        SettlementResultEvent settlement = new SettlementResultEvent(7001L, 1L, new BigDecimal("60000"));

        consumer.consume(settlement, ack);
        assertTrue(latch.await(1, TimeUnit.SECONDS), "1초 안에 매수 접수·정산 콜백이 도착해야 한다");

        assertEquals(3, events.size()); // 매수 접수 + 매수 체결 + 정산
        Recorded settlementEvent = events.get(2);
        assertEquals(7001L, settlementEvent.settlementRef());
        assertTrue(settlementEvent.applied());
        verify(ack).acknowledge();
    }

    private static final class Recorder implements AccountResultListener {
        private final List<Recorded> events;
        private final CountDownLatch latch;

        Recorder(List<Recorded> events, CountDownLatch latch) {
            this.events = events;
            this.latch = latch;
        }

        @Override
        public void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin) {
            events.add(new Recorded(accountId, false, null));
            latch.countDown();
        }

        @Override
        public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
            events.add(new Recorded(accountId, false, null));
            latch.countDown();
        }

        @Override
        public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
            events.add(new Recorded(accountId, false, null));
            latch.countDown();
        }

        @Override
        public void onFillApplied(long accountId, long orderId, long tradeId, boolean applied) {
            events.add(new Recorded(accountId, applied, null));
            latch.countDown();
        }

        @Override
        public void onSettlementApplied(long accountId, long settlementRef, boolean applied) {
            events.add(new Recorded(accountId, applied, settlementRef));
            latch.countDown();
        }

        @Override
        public void onUnpaidRecorded(long accountId, long tradeId, BigDecimal amount) {
        }

        @Override
        public void onDuplicateRequest(long accountId, long orderId, String requestId) {
        }
    }

    private record Recorded(long accountId, boolean applied, Long settlementRef) {
    }
}
