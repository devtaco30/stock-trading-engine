package com.flab.stocktradingengine.matching.disruptor.handler;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Map;
import java.util.Optional;

import com.lmax.disruptor.EventHandler;

import com.flab.stocktradingengine.trading.matching.FillResult;
import com.flab.stocktradingengine.trading.matching.OrderBook;
import com.flab.stocktradingengine.trading.matching.OrderEntry;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.matching.disruptor.engine.OrderEvent;
import com.flab.stocktradingengine.matching.disruptor.io.MatchListener;

/**
 * 링버퍼를 소비하는 단일 매칭 핸들러.
 *
 * <h3>단일 스레드 보장</h3>
 * <p>Disruptor 가 이 핸들러를 스레드 하나에만 배정하므로, 여기서 만지는 상태는
 * 한 스레드만 접근한다. 그래서 종목별 호가창을 담는 {@code books} 를 동시성 구조가 아닌
 * 일반 {@link HashMap} 으로 둔다. AS-IS 의 {@code OrderBookRegistry}(ConcurrentHashMap)와
 * 달리 동시 접근이 없어 락·동시성 맵이 필요 없다.</p>
 *
 * <h3>매칭 로직</h3>
 * <p>주문 접수·취소·체결 판단은 기존 {@link OrderBook} 을 그대로 재사용한다.
 * 이 핸들러는 링버퍼 이벤트를 {@link OrderEntry} 로 옮겨 OrderBook 에 넘기고,
 * 체결이 나오면 {@link MatchListener} 로 내보내는 얇은 껍데기다.</p>
 */
public class MatchingEventHandler implements EventHandler<OrderEvent> {

    private static final Logger log = System.getLogger(MatchingEventHandler.class.getName());

    // 종목코드 → 호가창. 단일 스레드만 접근하므로 일반 HashMap 으로 충분하다.
    // MatchingEngine 이 필드로 들고 있다가 넘겨준다 — recover(2c-2)가 새 핸들러 인스턴스로
    // 저널을 재적용할 때도 같은 맵을 공유해야 라이브 매칭이 그 위에서 이어진다
    // (account-disruptor AccountEventHandler 의 accounts 맵과 같은 이유).
    private final Map<String, OrderBook> books;
    private final MatchListener listener;

    public MatchingEventHandler(Map<String, OrderBook> books, MatchListener listener) {
        this.books = books;
        this.listener = listener;
    }

    @Override
    public void onEvent(OrderEvent event, long sequence, boolean endOfBatch) {
        try {
            if (event.getType() == EventType.PLACE) {
                handlePlace(event);
            } else if (event.getType() == EventType.CANCEL) {
                handleCancel(event);
            }
        } catch (IllegalArgumentException | IllegalStateException e) {
            // 도메인 불변식 위반(수량 초과·잘못된 상태 전이 등)은 재시도해도 같으므로 이 이벤트만 폐기한다.
            // 핸들러에서 예외를 밖으로 던지면 Disruptor 기본 처리기가 시퀀스를 멈추므로 여기서 잡아 넘긴다.
            log.log(Level.WARNING, "[매칭] 이벤트 폐기: type=" + event.getType()
                + " orderId=" + event.getOrderId() + " 이유=" + e.getMessage());
        } finally {
            // 슬롯 재사용 대비: 이 핸들러가 마지막 소비자이므로 처리 후 비운다.
            event.clear();
        }
    }

    private void handlePlace(OrderEvent event) {
        OrderBook book = books.computeIfAbsent(event.getStockCode(), k -> new OrderBook());
        if (book.containsOrder(event.getOrderId())) {
            // 멱등성: 같은 주문이 중복 도착하면 무시한다.
            return;
        }
        OrderEntry entry = toEntry(event);
        book.addOrder(entry);
        runMatch(event.getStockCode(), book);
    }

    private void handleCancel(OrderEvent event) {
        OrderBook book = books.get(event.getStockCode());
        if (book == null) {
            return;
        }
        book.cancelOrder(event.getOrderId());
    }

    private void runMatch(String stockCode, OrderBook book) {
        while (true) {
            Optional<FillResult> result = book.match();
            if (result.isEmpty()) {
                break;
            }
            listener.onFill(stockCode, result.get());
        }
    }

    private static OrderEntry toEntry(OrderEvent event) {
        return new OrderEntry(
            event.getOrderId(),
            event.getAccountId(),
            event.getStockCode(),
            event.getSide(),
            event.getPrice(),
            event.getQuantity(),
            event.getOrderAt()
        );
    }
}
