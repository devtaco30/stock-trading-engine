package com.flab.stocktradingengine.matching.disruptor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.StockSlotHasher;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * 복구가 저널의 넘김 항목을 살아 있는 핸들러와 같게 다룬다. 취소(CANCEL)는 수신 때 담당 확인을 거치지 않아 넘김 뒤에도 저널에
 * 들어갈 수 있고, 살아 있는 핸들러는 넘기는 중인 종목 slot의 이벤트를 호가창에 반영하지 않는다. 복구가 그 취소를 반영하면
 * 재시작 전후의 호가창이 갈린다.
 */
class MatchingEngineRecoverHandedOverTest {

    private static final StockSlotHasher HASHER = new StockSlotHasher(64);
    private static final String STOCK = "005930";

    private MatchingEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    private JournaledOrder place(long orderId) {
        return new JournaledOrder(EventType.PLACE, orderId, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now());
    }

    private JournaledOrder cancel(long orderId) {
        return new JournaledOrder(EventType.CANCEL, orderId, 0L, STOCK, null, null, 0, null);
    }

    @Test
    void 넘김_항목_뒤의_취소는_복구에서도_호가창에_반영되지_않는다() {
        engine = new MatchingEngine(1024, (stockCode, fill) -> { });

        engine.recover(List.of(place(1L), JournaledOrder.slotReleased(HASHER.slotFor(STOCK)), cancel(1L)));

        assertTrue(engine.containsOrder(STOCK, 1L), "넘긴 뒤에 온 취소는 살아 있는 핸들러처럼 반영하지 않는다");
    }

    @Test
    void 넘김_항목_앞의_취소는_반영된다() {
        engine = new MatchingEngine(1024, (stockCode, fill) -> { });

        engine.recover(List.of(place(1L), cancel(1L), JournaledOrder.slotReleased(HASHER.slotFor(STOCK))));

        assertFalse(engine.containsOrder(STOCK, 1L));
    }

    @Test
    void 넘긴_뒤_다시_받은_종목_slot의_취소는_반영된다() {
        engine = new MatchingEngine(1024, (stockCode, fill) -> { });

        engine.recover(List.of(place(1L), JournaledOrder.slotReleased(HASHER.slotFor(STOCK)),
            JournaledOrder.slotAcquired(HASHER.slotFor(STOCK)), cancel(1L)));

        assertFalse(engine.containsOrder(STOCK, 1L));
    }

    @Test
    void 복구가_끝난_뒤_시작한_엔진도_그_종목_slot을_넘기는_중으로_안다() throws Exception {
        engine = new MatchingEngine(1024, (stockCode, fill) -> { });
        engine.recover(List.of(place(1L), JournaledOrder.slotReleased(HASHER.slotFor(STOCK))));
        engine.start();

        engine.publishCancel(1L, STOCK);
        engine.publishPlace(2L, 100L, "000660", OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now());
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (!engine.containsOrder("000660", 2L) && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }

        assertTrue(engine.containsOrder(STOCK, 1L), "복구 뒤 라이브 취소도 넘기는 중이라 반영되지 않는다");
    }

    @Test
    void 복구를_두_번_나눠_해도_앞_복구의_넘김이_뒤_복구에_이어진다() {
        engine = new MatchingEngine(1024, (stockCode, fill) -> { });

        engine.recover(List.of(place(1L), JournaledOrder.slotReleased(HASHER.slotFor(STOCK))));
        engine.recover(List.of(cancel(1L)));

        assertTrue(engine.containsOrder(STOCK, 1L));
    }
}
