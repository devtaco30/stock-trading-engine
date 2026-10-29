package com.flab.stocktradingengine.matching.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.StockSlotHasher;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventListener;
import com.flab.stocktradingengine.matching.disruptor.journal.InMemoryJournal;
import com.flab.stocktradingengine.matching.disruptor.snapshot.BookSnapshot;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshot;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshotCodec;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.matching.FillResult;
import com.lmax.disruptor.BlockingWaitStrategy;

/**
 * 「slot 넘김」 이벤트(U13). 이벤트 앞의 주문은 호가창에 반영되고, 뒤에 온 같은 종목 slot의 주문은 반영되지 않으며,
 * 내보낸 바이트는 이벤트 시점의 호가창이고, 옮기지 않는 종목 slot은 그대로다.
 */
class MatchingEngineSlotHandoverTest {

    private static final StockSlotHasher HASHER = new StockSlotHasher(64);
    private static final String MOVED_STOCK = "005930";
    // 옮기는 종목과 종목 slot이 다른 종목 하나를 고른다.
    private static final String KEPT_STOCK = stockInOtherSlot();

    private final List<FillResult> fills = new CopyOnWriteArrayList<>();
    private final List<HandedOver> handedOver = new CopyOnWriteArrayList<>();
    private final CountDownLatch handedOverLatch = new CountDownLatch(1);
    private final MatchingSnapshotCodec codec = new MatchingSnapshotCodec();
    private MatchingEngine engine;

    private static String stockInOtherSlot() {
        for (int code = 1; code < 1000; code++) {
            String candidate = String.format("%06d", code);
            if (HASHER.slotFor(candidate) != HASHER.slotFor(MOVED_STOCK)) {
                return candidate;
            }
        }
        throw new IllegalStateException("종목 slot이 다른 종목을 찾지 못했다");
    }

    private record HandedOver(int stockSlot, byte[] bytes) {
    }

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    private void startEngine() {
        SlotEventListener listener = new SlotEventListener() {
            @Override
            public void onSlotHandedOver(int stockSlot, byte[] bookBytes) {
                handedOver.add(new HandedOver(stockSlot, bookBytes));
                handedOverLatch.countDown();
            }
        };
        engine = new MatchingEngine(1024, new BlockingWaitStrategy(), (stockCode, fill) -> fills.add(fill),
            new InMemoryJournal(), HASHER, listener);
        engine.start();
    }

    private int movedSlot() {
        return HASHER.slotFor(MOVED_STOCK);
    }

    @Test
    void 시험_종목_둘은_서로_다른_slot이다() {
        assertNotEquals(HASHER.slotFor(MOVED_STOCK), HASHER.slotFor(KEPT_STOCK));
    }

    @Test
    void 넘김_이벤트_앞에_들어간_주문은_모두_호가창에_반영된다() throws Exception {
        startEngine();
        engine.publishPlace(1L, 100L, MOVED_STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now());
        engine.publishPlace(2L, 100L, MOVED_STOCK, OrderSide.BUY, new BigDecimal("9900"), 5, Instant.now());

        engine.publishSlotReleased(movedSlot());
        awaitHandedOver();

        assertTrue(engine.containsOrder(MOVED_STOCK, 1L));
        assertTrue(engine.containsOrder(MOVED_STOCK, 2L));
    }

    @Test
    void 넘김_이벤트_뒤에_들어온_같은_종목_slot의_주문은_호가창에_반영되지_않는다() throws Exception {
        startEngine();
        engine.publishSlotReleased(movedSlot());
        engine.publishPlace(3L, 100L, MOVED_STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now());
        // 뒤따르는 옮기지 않는 종목의 주문이 반영된 시점에는 앞의 주문도 이미 처리됐다(엔진 thread는 하나다).
        engine.publishPlace(4L, 100L, KEPT_STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now());
        awaitContains(KEPT_STOCK, 4L);

        assertFalse(engine.containsOrder(MOVED_STOCK, 3L));
    }

    @Test
    void 내보낸_바이트를_디코딩하면_이벤트_시점의_호가창과_같다() throws Exception {
        startEngine();
        Instant now = Instant.now();
        engine.publishPlace(1L, 100L, MOVED_STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, now);
        engine.publishPlace(2L, 200L, MOVED_STOCK, OrderSide.SELL, new BigDecimal("10000"), 4, now.plusMillis(1));
        engine.publishPlace(3L, 300L, MOVED_STOCK, OrderSide.SELL, new BigDecimal("11000"), 7, now.plusMillis(2));

        engine.publishSlotReleased(movedSlot());
        awaitHandedOver();

        HandedOver sent = handedOver.get(0);
        assertEquals(movedSlot(), sent.stockSlot());
        MatchingSnapshot decoded = codec.decode(sent.bytes());
        BookSnapshot book = decoded.booksByStock().get(MOVED_STOCK);
        assertEquals(2, book.restingOrders().size(), "매수 1(잔량 6)과 매도 3(잔량 7)이 남는다");
        assertEquals(6, book.restingOrders().stream().filter(o -> o.orderId() == 1L).findFirst().orElseThrow().quantity()
            - book.restingOrders().stream().filter(o -> o.orderId() == 1L).findFirst().orElseThrow().filledQuantity());
        assertTrue(book.filledOrderTimestampsEpochMillis().containsKey(2L), "전량 체결된 매도 2의 체결 시각 기록이 담겨야 한다");
    }

    @Test
    void 옮기지_않는_종목의_호가창은_그대로이고_바이트에도_담기지_않는다() throws Exception {
        startEngine();
        engine.publishPlace(1L, 100L, KEPT_STOCK, OrderSide.SELL, new BigDecimal("10000"), 10, Instant.now());
        engine.publishPlace(2L, 100L, MOVED_STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now());

        engine.publishSlotReleased(movedSlot());
        awaitHandedOver();
        engine.publishPlace(5L, 200L, KEPT_STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now());
        awaitFill();

        MatchingSnapshot decoded = codec.decode(handedOver.get(0).bytes());
        assertNull(decoded.booksByStock().get(KEPT_STOCK), "옮기지 않는 종목은 바이트에 없다");
        assertEquals(1L, fills.get(0).sellOrderId(), "옮기지 않는 종목의 호가창은 계속 체결에 쓰인다");
    }

    private void awaitHandedOver() throws InterruptedException {
        assertTrue(handedOverLatch.await(3, TimeUnit.SECONDS), "3초 안에 호가창 바이트가 나오지 않음");
    }

    private void awaitFill() throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (fills.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertFalse(fills.isEmpty(), "3초 안에 체결이 나오지 않음");
    }

    private void awaitContains(String stockCode, long orderId) throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (!engine.containsOrder(stockCode, orderId) && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertTrue(engine.containsOrder(stockCode, orderId), "3초 안에 주문이 반영되지 않음: " + orderId);
    }
}
