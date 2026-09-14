package com.flab.stocktradingengine.account.disruptor.io;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.account.disruptor.domain.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.domain.RejectReason;
import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.codec.FillCodec;
import com.flab.stocktradingengine.codec.FilledTrade;

/**
 * 체결 하나를 디코딩하면 매수·매도 계좌 양쪽에 전부 반영하는지, 손상 프레임은 예외 없이
 * skip하는지 검증한다(ADR-032, U3). {@code AccountFillConsumerTest}(U2 이전 Kafka 구현)와
 * 같은 결이다.
 */
class AccountFillReceiverTest {

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
        engine = new AccountEngine(BUFFER_SIZE, 0L,
            (orderId, accountId, stockCode, side, price, quantity) -> {}, new Recorder(events, latch));
    }

    private void awaitResults() throws InterruptedException {
        assertTrue(latch.await(1, TimeUnit.SECONDS), "1초 안에 결과가 도착해야 한다");
    }

    @Test
    @DisplayName("체결 하나를 디코딩하면 매수·매도 양쪽에 전부 반영한다")
    void 체결하나_디코딩하면_양쪽반영() throws InterruptedException {
        prepare(4); // 매수 예약(1) + 매도 예약(1) + 매수 체결반영(1) + 매도 체결반영(1)
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40")); // 매수 계좌
        engine.seed(2L, new BigDecimal("1000000"), new BigDecimal("0.40"), Map.of(STOCK, 10)); // 매도 계좌(보유 10)
        engine.start();

        AccountFillReceiver receiver = new AccountFillReceiver(null, engine);

        // orderId(1, 2)는 이 엔진(nodeId=0)이 발급할 순번을 예측한 값이다(2b-0, 결정론적 카운터).
        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 4, "r1"); // orderId=1
        engine.publishSell(2L, STOCK, new BigDecimal("10000"), 4, "r2"); // orderId=2

        FilledTrade trade = new FilledTrade(9001L, STOCK, 1L, 1L, 2L, 2L, 4, new BigDecimal("10000"));
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        int length = new FillCodec().encode(buffer, 0, trade);
        receiver.onFragment(buffer, 0, length, null);
        awaitResults(); // 매수·매도 예약이 처리될 스레드가 같아 순서가 보장된다

        assertEquals(4, events.size());
        Recorded buyFillEvent = events.get(2);
        Recorded sellFillEvent = events.get(3);
        assertEquals(9001L, buyFillEvent.tradeId());
        assertTrue(buyFillEvent.success());
        assertEquals(9001L, sellFillEvent.tradeId());
        assertTrue(sellFillEvent.success());
    }

    @Test
    @DisplayName("손상된 체결 프레임은 tryDecode가 빈 값을 돌려줘 예외 없이 skip한다")
    void 손상프레임은_예외없이_skip() throws InterruptedException {
        prepare(1); // 매수 예약(1)만 — 손상 프레임에 대한 체결 반영은 오지 않아야 한다
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        AccountFillReceiver receiver = new AccountFillReceiver(null, engine);
        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 4, "r1");
        awaitResults();

        UnsafeBuffer poison = new UnsafeBuffer(new byte[4]); // FilledTrade 최소 길이(46+)에 한참 못 미침
        assertDoesNotThrow(() -> receiver.onFragment(poison, 0, poison.capacity(), null));

        assertEquals(1, events.size()); // 추가 콜백 없음 — 손상 프레임이 조용히 skip됐다
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

        @Override
        public void onSettlementApplied(long accountId, long settlementRef, boolean applied) {
            events.add(new Recorded(accountId, 0L, applied, settlementRef, null));
            latch.countDown();
        }

        @Override
        public void onUnpaidRecorded(long accountId, long tradeId, BigDecimal amount) {
        }

        @Override
        public void onDuplicateRequest(long accountId, long orderId, String requestId) {
        }
    }

    private record Recorded(long accountId, long orderId, boolean success, Long tradeId, RejectReason reason) {
    }
}
