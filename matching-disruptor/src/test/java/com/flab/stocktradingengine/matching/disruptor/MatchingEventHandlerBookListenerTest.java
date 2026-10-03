package com.flab.stocktradingengine.matching.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.matching.disruptor.engine.OrderEvent;
import com.flab.stocktradingengine.matching.disruptor.handler.MatchingEventHandler;
import com.flab.stocktradingengine.matching.disruptor.io.MatchListener;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.matching.FillResult;
import com.flab.stocktradingengine.trading.matching.OrderBook;

/**
 * 호가창 스냅샷을 Redis에 기록하려고 매칭 핸들러가 listener에 「호가창이 바뀌었다」를 알리는 지점. 이벤트마다가 아니라
 * 배치의 마지막 이벤트에서 건드린 종목마다 한 번만 알린다.
 */
class MatchingEventHandlerBookListenerTest {

    private static final String STOCK_A = "A000001";
    private static final String STOCK_B = "A000002";

    private final List<String> changed = new ArrayList<>();
    private final Map<String, OrderBook> books = new HashMap<>();
    private final MatchListener listener = new MatchListener() {
        @Override
        public void onFill(String stockCode, FillResult fill) { }

        @Override
        public void onBookChanged(String stockCode, OrderBook book) {
            changed.add(stockCode);
            assertSame(books.get(stockCode), book, "그 종목의 호가창을 넘겨야 한다");
        }
    };
    private final MatchingEventHandler handler = new MatchingEventHandler(books, listener);

    private static void place(MatchingEventHandler handler, long orderId, String stock, OrderSide side, String price, long seq,
                              boolean endOfBatch) {
        OrderEvent event = new OrderEvent();
        event.setPlace(orderId, 1L, stock, side, new BigDecimal(price), 1, Instant.now());
        handler.onEvent(event, seq, endOfBatch);
    }

    @Test
    @DisplayName("배치가 이벤트 하나씩이면 이벤트마다 그 종목으로 한 번씩 알린다")
    void 이벤트_하나씩이면_이벤트마다_알린다() {
        place(handler, 1, STOCK_A, OrderSide.BUY, "100", 0, true);
        place(handler, 2, STOCK_A, OrderSide.SELL, "200", 1, true);

        assertEquals(List.of(STOCK_A, STOCK_A), changed);
    }

    @Test
    @DisplayName("한 배치에 같은 종목 이벤트가 여럿이면 배치 끝에서 한 번만 알린다")
    void 한_배치의_같은_종목은_한_번만_알린다() {
        place(handler, 1, STOCK_A, OrderSide.BUY, "100", 0, false);
        place(handler, 2, STOCK_A, OrderSide.BUY, "99", 1, false);
        assertTrue(changed.isEmpty(), "배치 중간에는 알리지 않는다");
        place(handler, 3, STOCK_A, OrderSide.SELL, "200", 2, true);

        assertEquals(List.of(STOCK_A), changed);
    }

    @Test
    @DisplayName("한 배치에서 여러 종목을 건드리면 종목마다 한 번씩 알리고, 다음 배치에는 이어지지 않는다")
    void 여러_종목은_종목마다_한_번씩_알리고_다음_배치로_넘어가지_않는다() {
        place(handler, 1, STOCK_A, OrderSide.BUY, "100", 0, false);
        place(handler, 2, STOCK_B, OrderSide.BUY, "100", 1, true);
        assertEquals(2, changed.size());
        assertTrue(changed.containsAll(List.of(STOCK_A, STOCK_B)));

        changed.clear();
        place(handler, 3, STOCK_B, OrderSide.SELL, "300", 2, true);
        assertEquals(List.of(STOCK_B), changed);
    }

    @Test
    @DisplayName("같은 주문이 중복 도착해도 그 종목은 알린다(v1이 중복 이벤트에도 스냅샷을 쓰는 것과 같다)")
    void 중복_주문이어도_알린다() {
        place(handler, 1, STOCK_A, OrderSide.BUY, "100", 0, true);
        changed.clear();

        place(handler, 1, STOCK_A, OrderSide.BUY, "100", 1, true);

        assertEquals(List.of(STOCK_A), changed);
    }

    @Test
    @DisplayName("취소는 호가창이 있는 종목만 알리고, 호가창이 없는 종목은 알리지 않는다")
    void 취소는_호가창이_있을_때만_알린다() {
        place(handler, 1, STOCK_A, OrderSide.BUY, "100", 0, true);
        changed.clear();

        OrderEvent cancelKnown = new OrderEvent();
        cancelKnown.setCancel(1L, STOCK_A);
        handler.onEvent(cancelKnown, 1, true);
        assertEquals(List.of(STOCK_A), changed);

        changed.clear();
        OrderEvent cancelUnknown = new OrderEvent();
        cancelUnknown.setCancel(9L, STOCK_B);
        handler.onEvent(cancelUnknown, 2, true);
        assertTrue(changed.isEmpty(), "호가창이 없는 종목은 알리지 않는다");
    }

    @Test
    @DisplayName("listener가 예외를 던져도 핸들러는 다음 이벤트를 계속 처리한다")
    void listener_예외가_핸들러를_멈추지_않는다() {
        MatchListener throwing = new MatchListener() {
            @Override
            public void onFill(String stockCode, FillResult fill) { }

            @Override
            public void onBookChanged(String stockCode, OrderBook book) {
                throw new IllegalStateException("boom");
            }
        };
        MatchingEventHandler h = new MatchingEventHandler(books, throwing);

        place(h, 1, STOCK_A, OrderSide.BUY, "100", 0, true);
        place(h, 2, STOCK_A, OrderSide.SELL, "100", 1, true);

        assertTrue(books.get(STOCK_A).getBidLevels(10).isEmpty(), "두 번째 이벤트(체결)까지 처리돼 호가창이 비어야 한다");
    }
}
