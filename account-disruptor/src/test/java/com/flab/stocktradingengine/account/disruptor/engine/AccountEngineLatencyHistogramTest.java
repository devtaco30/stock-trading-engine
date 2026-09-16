package com.flab.stocktradingengine.account.disruptor.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.account.disruptor.domain.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.domain.RejectReason;
import com.flab.stocktradingengine.account.disruptor.io.AccountSnapshotSink;
import com.flab.stocktradingengine.account.disruptor.io.MatchingOrderSender;
import com.flab.stocktradingengine.account.disruptor.journal.InMemoryAccountJournal;
import com.flab.stocktradingengine.time.EpochNanos;
import com.flab.stocktradingengine.time.LatencyHistogram;
import com.flab.stocktradingengine.time.LatencySnapshot;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * 끝점①(접수·예약) 접수 지연 측정(decision_records/v1-v2-e2e-measurement.md) — accept된 매수·매도만
 * {@link LatencyHistogram}에 기록되고, 거부·중복은 모집단에서 빠지는지 검증한다.
 *
 * <p>동기화는 {@code accountState().seq()} 폴링이 아니라 리스너 콜백({@link CountDownLatch})으로
 * 한다 — 핸들러 안에서 record()가 listener.onAccepted 호출 직전에 일어나므로, latch.await()가
 * 돌아온 시점엔 record()가 이미 끝나 있다는 happens-before가 성립한다. seq 폴링은 그 보장이 없다
 * (seq++ 도 record() 이전에 일어나 폴링 스레드가 record() 완료 전에 snapshot을 읽을 수 있다).</p>
 */
class AccountEngineLatencyHistogramTest {

    private static final long NODE_ID = 0L;
    private static final String STOCK = "005930";
    private static final MatchingOrderSender NO_OP_SENDER = (orderId, accountId, stockCode, side, price, quantity) -> {};
    private static final AccountSnapshotSink NO_OP_SINK = new AccountSnapshotSink() {
        @Override
        public boolean offer(byte[] snapshotBytes, Map<Integer, Long> fillPositions, long appliedSeq) {
            return true;
        }

        @Override
        public long durableSeq() {
            return 0L;
        }
    };

    private AccountEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    @Test
    @DisplayName("매수가 accept되면 발행 시각부터의 경과가 히스토그램에 기록된다")
    void 매수_accept시_접수_지연을_기록한다() throws InterruptedException {
        LatencyHistogram latencyHistogram = new LatencyHistogram(true);
        CountingListener listener = new CountingListener(1);
        engine = newEngine(latencyHistogram, listener);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        long publishedAt = EpochNanos.now() - TimeUnit.MILLISECONDS.toNanos(10);
        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 1, "r1", publishedAt);
        assertTrue(listener.await(), "2초 안에 accept 콜백이 와야 한다");

        LatencySnapshot snapshot = latencyHistogram.snapshotAndReset();
        assertEquals(1, snapshot.count());
        assertTrue(snapshot.p50Nanos() >= TimeUnit.MILLISECONDS.toNanos(10), "기록값이 실제 경과시간 이상이어야 한다");
    }

    @Test
    @DisplayName("매도가 accept되면 발행 시각부터의 경과가 히스토그램에 기록된다")
    void 매도_accept시_접수_지연을_기록한다() throws InterruptedException {
        LatencyHistogram latencyHistogram = new LatencyHistogram(true);
        CountingListener listener = new CountingListener(1);
        engine = newEngine(latencyHistogram, listener);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"), Map.of(STOCK, 10));
        engine.start();

        long publishedAt = EpochNanos.now() - TimeUnit.MILLISECONDS.toNanos(5);
        engine.publishSell(1L, STOCK, new BigDecimal("10000"), 4, "r-sell", publishedAt);
        assertTrue(listener.await(), "2초 안에 accept 콜백이 와야 한다");

        assertEquals(1, latencyHistogram.snapshotAndReset().count());
    }

    @Test
    @DisplayName("거부된 주문(INVALID_QUANTITY)은 히스토그램에 기록되지 않는다")
    void 거부되면_기록하지_않는다() throws InterruptedException {
        LatencyHistogram latencyHistogram = new LatencyHistogram(true);
        // 거부 1건 + accept 1건 — 거부 콜백까지 기다려 처리 순서를 확정한 뒤 accept를 센다.
        CountingListener listener = new CountingListener(2);
        engine = newEngine(latencyHistogram, listener);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 0, "r-invalid"); // quantity 0 → INVALID_QUANTITY 거부
        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 1, "r-valid");
        assertTrue(listener.await(), "2초 안에 콜백 2건(거부+accept)이 와야 한다");

        assertEquals(1, latencyHistogram.snapshotAndReset().count(), "거부(r-invalid)는 안 세고 accept(r-valid)만 세야 한다");
    }

    @Test
    @DisplayName("재전송(중복 requestId)은 히스토그램에 다시 기록되지 않는다")
    void 중복이면_다시_기록하지_않는다() throws InterruptedException {
        LatencyHistogram latencyHistogram = new LatencyHistogram(true);
        // accept 2건(r1, r2) + 중복통지 1건(r1 재도착) — 총 콜백 3건을 기다린다.
        CountingListener listener = new CountingListener(3);
        engine = newEngine(latencyHistogram, listener);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 1, "r1");
        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 1, "r1"); // 재전송
        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 1, "r2");
        assertTrue(listener.await(), "2초 안에 콜백 3건이 와야 한다");

        assertEquals(2, latencyHistogram.snapshotAndReset().count(), "재전송(r1 재도착)은 안 세고 첫 accept 2건(r1, r2)만 세야 한다");
    }

    @Test
    @DisplayName("측정이 꺼져 있으면(disabled) accept돼도 기록되지 않는다")
    void 꺼져있으면_accept돼도_기록_안한다() throws InterruptedException {
        LatencyHistogram latencyHistogram = new LatencyHistogram(false);
        CountingListener listener = new CountingListener(1);
        engine = newEngine(latencyHistogram, listener);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 1, "r1");
        assertTrue(listener.await(), "2초 안에 accept 콜백이 와야 한다");

        assertEquals(LatencySnapshot.empty(), latencyHistogram.snapshotAndReset());
    }

    private AccountEngine newEngine(LatencyHistogram latencyHistogram, AccountResultListener listener) {
        return new AccountEngine(1024, new BlockingWaitStrategy(), ProducerType.SINGLE, NODE_ID,
            NO_OP_SENDER, listener, new InMemoryAccountJournal(), NO_OP_SINK, latencyHistogram);
    }

    /** onAccepted·onSellAccepted·onRejected·onDuplicateRequest — 무엇이든 콜백이 오면 래치를 내린다. */
    private static final class CountingListener implements AccountResultListener {
        private final CountDownLatch latch;

        CountingListener(int expectedCallbacks) {
            this.latch = new CountDownLatch(expectedCallbacks);
        }

        boolean await() throws InterruptedException {
            return latch.await(2, TimeUnit.SECONDS);
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
        }

        @Override
        public void onSettlementApplied(long accountId, long settlementRef, boolean applied) {
        }

        @Override
        public void onUnpaidRecorded(long accountId, long tradeId, BigDecimal amount) {
        }

        @Override
        public void onDuplicateRequest(long accountId, String requestId) {
            latch.countDown();
        }
    }
}
