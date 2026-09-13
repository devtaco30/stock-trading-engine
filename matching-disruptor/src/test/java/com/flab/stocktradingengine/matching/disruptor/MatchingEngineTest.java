package com.flab.stocktradingengine.matching.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.lmax.disruptor.BlockingWaitStrategy;

import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.matching.FillResult;

/**
 * Unit 1 매칭 파이프라인 검증.
 *
 * <p>프로듀서(테스트)와 소비자(매칭 핸들러)가 서로 다른 스레드에서 돌기 때문에,
 * 발행 직후 결과가 준비돼 있지 않다. {@link CountDownLatch} 로 체결 도착을 기다린 뒤 검증한다.</p>
 */
class MatchingEngineTest {

    private static final String STOCK = "005930";
    private static final int BUFFER_SIZE = 1024;

    private final List<FillResult> fills = new CopyOnWriteArrayList<>();
    private MatchingEngine engine;
    private CountDownLatch latch;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    private void startEngine(int expectedFills) {
        latch = new CountDownLatch(expectedFills);
        engine = new MatchingEngine(BUFFER_SIZE, (stockCode, fill) -> {
            fills.add(fill);
            latch.countDown();
        });
        engine.start();
    }

    @Test
    void 교차_주문은_체결된다() throws InterruptedException {
        startEngine(1);
        Instant now = Instant.now();

        engine.publishPlace(1L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 100, now);
        engine.publishPlace(2L, 200L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 100, now.plusMillis(1));

        boolean arrived = latch.await(1, TimeUnit.SECONDS);

        assertTrue(arrived, "1초 안에 체결이 도착해야 한다");
        assertEquals(1, fills.size());
        FillResult fill = fills.get(0);
        assertEquals(1L, fill.buyOrderId());
        assertEquals(2L, fill.sellOrderId());
        assertEquals(100, fill.filledQuantity());
        assertEquals(0, new BigDecimal("10000").compareTo(fill.matchPrice()));
    }

    @Test
    void 가격이_안_맞으면_체결되지_않는다() throws InterruptedException {
        startEngine(1);
        Instant now = Instant.now();

        // 매수 9000 < 매도 10000 → 체결 불가
        engine.publishPlace(1L, 100L, STOCK, OrderSide.BUY, new BigDecimal("9000"), 100, now);
        engine.publishPlace(2L, 200L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 100, now.plusMillis(1));

        boolean arrived = latch.await(300, TimeUnit.MILLISECONDS);

        assertFalse(arrived, "체결이 없어야 한다");
        assertEquals(0, fills.size());
    }

    // ---------- shutdown 안전성 (죽은 소비자 앞 무한 대기 방지) ----------

    /**
     * 저널 소비자(1단계)가 죽으면 저널 소비자 자신은 Disruptor의 hasBacklog 검사에서 빠진다
     * (BatchEventProcessor.run()의 finally가 running을 IDLE로 되돌려 "죽은 소비자"가 "안 도는
     * 소비자"와 구분이 안 됨) — 그래서 뒤에 물린 매칭 핸들러(체인의 마지막, hasBacklog가 실제로
     * 보는 대상)가 죽은 저널 소비자의 시퀀스를 영원히 기다리며 블록된 채(죽지 않고 running=true)
     * 멈춰야 shutdown()이 실제로 무한 대기한다. 매칭 핸들러 자신이 죽는 시나리오로는 이 hang이
     * 재현되지 않는다(그 핸들러도 죽으면 검사 대상에서 빠져 shutdown()이 바로 반환됨 —
     * Disruptor 4.0.0 소스로 확인).
     */
    @Test
    void 저널소비자가_죽어_핸들러가_블록돼도_shutdown은_타임아웃_안에_반환() throws InterruptedException {
        CountDownLatch journalReached = new CountDownLatch(1);
        CountDownLatch shutdownReturned = new CountDownLatch(1);

        MatchingEngine deadJournalEngine = new MatchingEngine(BUFFER_SIZE, new BlockingWaitStrategy(),
            (stockCode, fill) -> { }, new PoisonJournal(journalReached));
        deadJournalEngine.start();

        Instant now = Instant.now();
        deadJournalEngine.publishPlace(1L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 100, now);
        assertTrue(journalReached.await(1, TimeUnit.SECONDS), "저널 소비자가 append에 도달해야 한다");

        Thread shutdownThread = new Thread(() -> {
            deadJournalEngine.shutdown();
            shutdownReturned.countDown();
        });
        shutdownThread.setDaemon(true);
        shutdownThread.start();

        assertTrue(shutdownReturned.await(8, TimeUnit.SECONDS), "8초 안에 shutdown()이 반환해야 한다");
    }

    /** append 호출 시 latch를 내리고 예외를 던져 저널 소비자를 죽이는 테스트용 저널(shutdown 안전성 검증). */
    private static final class PoisonJournal implements Journal {
        private final CountDownLatch reached;

        PoisonJournal(CountDownLatch reached) {
            this.reached = reached;
        }

        @Override
        public void append(JournaledOrder order) {
            reached.countDown();
            throw new RuntimeException("의도적 저널 소비자 사망(테스트)");
        }

        @Override
        public List<JournaledOrder> entries() {
            return List.of();
        }

        @Override
        public long position() {
            return 0L;
        }
    }
}
