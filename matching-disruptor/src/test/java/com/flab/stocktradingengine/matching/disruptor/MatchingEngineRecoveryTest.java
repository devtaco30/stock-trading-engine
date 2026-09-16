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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.io.MatchListener;
import com.flab.stocktradingengine.matching.disruptor.journal.InMemoryJournal;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.matching.FillResult;
import com.lmax.disruptor.BlockingWaitStrategy;

/**
 * 2c-2 — 저널 리플레이 복구({@link MatchingEngine#recover}) 검증. Aeron 없이 인메모리 저널만으로,
 * "라이브로 만든 호가창 == 저널을 재적용해 복구한 호가창"을 증명한다. account-disruptor의
 * {@code AccountEngineRecoveryTest}와 같은 결이다. Archive에서 실제로 읽어오는 배선은
 * 2c-2 matching-worker 몫이다.
 */
class MatchingEngineRecoveryTest {

    private static final String STOCK = "005930";
    private static final int BUFFER_SIZE = 1024;
    private static final MatchListener NO_OP_LISTENER = (stockCode, fill) -> {};

    private MatchingEngine original;
    private MatchingEngine recovered;

    @AfterEach
    void tearDown() {
        if (original != null) {
            original.shutdown();
        }
        if (recovered != null) {
            recovered.shutdown();
        }
    }

    @Test
    @DisplayName("저널을 재적용하면 미체결 주문이 원본과 같이 복원된다")
    void 저널_재적용하면_미체결_주문이_복원된다() {
        InMemoryJournal journal = new InMemoryJournal();
        original = new MatchingEngine(BUFFER_SIZE, new BlockingWaitStrategy(), NO_OP_LISTENER, journal);
        original.start();
        original.publishPlace(1L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now());
        awaitContainsOrder(original, 1L);

        List<JournaledOrder> entries = journal.entries();

        recovered = new MatchingEngine(BUFFER_SIZE, NO_OP_LISTENER);
        recovered.recover(entries);
        recovered.start();

        assertTrue(recovered.containsOrder(STOCK, 1L), "리플레이 후에도 미체결 주문이 남아있어야 한다");
    }

    @Test
    @DisplayName("recover 중에는 엔진에 넘긴 리스너가 한 번도 안 불린다(부수효과 없음)")
    void recover_중엔_리스너가_안_불린다() throws InterruptedException {
        List<JournaledOrder> entries = List.of(
            new JournaledOrder(EventType.PLACE, 1L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now()),
            new JournaledOrder(EventType.PLACE, 2L, 200L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 10, Instant.now())
        );

        List<FillResult> fills = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        recovered = new MatchingEngine(BUFFER_SIZE, (stockCode, fill) -> {
            fills.add(fill);
            latch.countDown();
        });
        recovered.recover(entries);

        assertTrue(fills.isEmpty(), "recover 중엔 엔진 리스너가 호출되면 안 된다");

        recovered.start();
        // 리플레이가 1·2를 이미 매칭했다면(no-op 리스너로만), 반대쪽 새 주문을 넣어도
        // 매칭될 잔량이 없어 체결이 나면 안 된다 — 남은 리스너로 그걸 확인한다.
        recovered.publishPlace(3L, 300L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 5, Instant.now());
        boolean arrived = latch.await(300, TimeUnit.MILLISECONDS);

        assertFalse(arrived, "리플레이로 이미 전량 매칭된 1·2에는 매칭될 잔량이 남아있지 않아야 한다");
        assertTrue(fills.isEmpty());
    }

    @Test
    @DisplayName("취소된 주문은 리플레이에도 반영돼 복원되지 않는다")
    void 취소된_주문은_복원되지_않는다() {
        List<JournaledOrder> entries = List.of(
            new JournaledOrder(EventType.PLACE, 1L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now()),
            new JournaledOrder(EventType.CANCEL, 1L, 0L, STOCK, null, null, 0, null)
        );

        recovered = new MatchingEngine(BUFFER_SIZE, NO_OP_LISTENER);
        recovered.recover(entries);
        recovered.start();

        assertFalse(recovered.containsOrder(STOCK, 1L), "취소된 주문은 복원되지 않아야 한다");
    }

    @Test
    @DisplayName("복구 뒤에도 라이브 매칭이 정상 동작한다")
    void 복구_뒤_라이브_매칭이_정상_동작한다() throws InterruptedException {
        List<JournaledOrder> entries = List.of(
            new JournaledOrder(EventType.PLACE, 1L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now())
        );

        List<FillResult> fills = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        recovered = new MatchingEngine(BUFFER_SIZE, (stockCode, fill) -> {
            fills.add(fill);
            latch.countDown();
        });
        recovered.recover(entries);
        recovered.start();

        recovered.publishPlace(2L, 200L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 10, Instant.now());

        assertTrue(latch.await(1, TimeUnit.SECONDS), "복원된 미체결 주문과 새 주문이 매칭돼야 한다");
        assertEquals(1L, fills.get(0).buyOrderId(), "복원된 주문(1)이 매수측이어야 한다");
        assertEquals(2L, fills.get(0).sellOrderId());
    }

    @Test
    @DisplayName("recover를 두 번(저널·인테이크) 연달아 불러도 겹치는 주문은 한 번만 반영된다")
    void recover를_두번_불러도_겹치는_주문은_한번만_반영된다() throws InterruptedException {
        // 저널 리플레이(이미 반영·저널까지 된 주문)와 인테이크 리플레이(반영 여부가 불확실한
        // 주문) 사이에 경합 구간이 생기면 같은 orderId가 양쪽에 다 들어올 수 있다(I2 U2b LLD
        // §4-1) — OrderBook.containsOrder의 기존 멱등 체크가 그 경우에도 지켜지는지 확인한다.
        //
        // containsOrder는 "최근 전량 체결"도 true로 취급하는 멱등 캐시라(OrderBook 106행)
        // 체결 후 상태를 직접 구분할 수 없다 — 대신 recover 뒤 라이브로 전환해, 1이 두 번
        // 반영됐다면 남았을 잔량(BUY 10)을 새 매도 주문으로 건드려 체결 유무로 확인한다
        // ("recover_중엔_리스너가_안_불린다" 테스트와 같은 기법).
        List<JournaledOrder> journalEntries = List.of(
            new JournaledOrder(EventType.PLACE, 1L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now())
        );
        List<JournaledOrder> intakeEntries = List.of(
            new JournaledOrder(EventType.PLACE, 1L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now()), // 겹침
            new JournaledOrder(EventType.PLACE, 2L, 200L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 10, Instant.now())
        );

        List<FillResult> fills = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        recovered = new MatchingEngine(BUFFER_SIZE, (stockCode, fill) -> {
            fills.add(fill);
            latch.countDown();
        });
        recovered.recover(journalEntries);
        recovered.recover(intakeEntries);
        recovered.start();

        recovered.publishPlace(3L, 300L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 1, Instant.now());
        boolean arrived = latch.await(300, TimeUnit.MILLISECONDS);

        assertFalse(arrived,
            "1이 두 번 반영됐다면 2(수량 10)와 상계되고도 BUY 10 잔량이 남아 방금 넣은 SELL 1이 체결돼야 한다 — "
                + "한 번만 반영됐어야 1·2가 정확히 상계돼 매칭될 잔량이 없다");
        assertTrue(fills.isEmpty());
    }

    /** 매칭 소비자 스레드가 주문을 호가창에 반영할 때까지 기다린다. */
    private void awaitContainsOrder(MatchingEngine engine, long orderId) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (!engine.containsOrder(STOCK, orderId)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("1초 안에 주문이 호가창에 반영되지 않음: orderId=" + orderId);
            }
            Thread.onSpinWait();
        }
    }
}
