package com.flab.stocktradingengine.matching.disruptor.snapshot;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.flab.stocktradingengine.trading.matching.OrderBook;
import com.flab.stocktradingengine.trading.matching.OrderEntry;

/**
 * books 맵으로부터 {@link MatchingSnapshot}을 만든다(I6 U1). 정상 종료 시 호출 스레드에서 찍는
 * {@code MatchingEngine#snapshot()}과, 소비자 스레드가 N건마다 찍는 러닝 중 스냅샷(I6 U1,
 * {@code MatchingEventHandler}) 양쪽이 같은 인코딩 로직을 쓴다 — 호출 스레드만 다르고 books를
 * 읽는 방식은 같다.
 */
public final class MatchingSnapshotFactory {

    private MatchingSnapshotFactory() {
    }

    public static MatchingSnapshot capture(Map<String, OrderBook> books, long journalPosition) {
        Map<String, BookSnapshot> booksByStock = new HashMap<>();
        for (Map.Entry<String, OrderBook> entry : books.entrySet()) {
            String stockCode = entry.getKey();
            OrderBook book = entry.getValue();

            List<RestingOrder> restingOrders = book.restingOrders().stream()
                .map(MatchingSnapshotFactory::toRestingOrder)
                .toList();

            Map<Long, Long> filledTimestampsEpochMillis = new HashMap<>();
            book.filledOrderTimestamps().forEach((orderId, filledAt) ->
                filledTimestampsEpochMillis.put(orderId, filledAt.toEpochMilli()));

            booksByStock.put(stockCode, new BookSnapshot(restingOrders, filledTimestampsEpochMillis));
        }
        return new MatchingSnapshot(booksByStock, journalPosition);
    }

    private static RestingOrder toRestingOrder(OrderEntry entry) {
        return new RestingOrder(entry.getOrderId(), entry.getAccountId(), entry.getSide(), entry.getPrice(),
            entry.getQuantity(), entry.getOrderAt().toEpochMilli(), entry.getFilledQuantity(), entry.isCancelled());
    }
}
