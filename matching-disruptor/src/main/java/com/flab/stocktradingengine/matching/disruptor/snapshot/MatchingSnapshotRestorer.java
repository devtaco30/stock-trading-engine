package com.flab.stocktradingengine.matching.disruptor.snapshot;

import java.time.Instant;
import java.util.Map;

import com.flab.stocktradingengine.trading.matching.OrderBook;
import com.flab.stocktradingengine.trading.matching.OrderEntry;

/**
 * {@link MatchingSnapshot}의 호가창을 books 맵에 되살린다. 엔진 시작 전 복원({@code MatchingEngine#restore})과 실행 중
 * 호가창 적재(호가창 이동을 받는 쪽, 엔진 thread)가 같은 변환을 쓴다. 호출하는 thread가 books 맵을 혼자 쓰는 때에만
 * 불러야 한다.
 */
public final class MatchingSnapshotRestorer {

    private MatchingSnapshotRestorer() {
    }

    public static void restoreInto(Map<String, OrderBook> books, MatchingSnapshot snapshot) {
        snapshot.booksByStock().forEach((stockCode, bookSnapshot) -> {
            OrderBook book = books.computeIfAbsent(stockCode, k -> new OrderBook());
            for (RestingOrder restingOrder : bookSnapshot.restingOrders()) {
                book.restoreRestingOrder(toOrderEntry(stockCode, restingOrder));
            }
            bookSnapshot.filledOrderTimestampsEpochMillis().forEach((orderId, epochMillis) ->
                book.restoreFilledOrderTimestamp(orderId, Instant.ofEpochMilli(epochMillis)));
        });
    }

    private static OrderEntry toOrderEntry(String stockCode, RestingOrder restingOrder) {
        OrderEntry entry = new OrderEntry(restingOrder.orderId(), restingOrder.accountId(), stockCode,
            restingOrder.side(), restingOrder.price(), restingOrder.quantity(),
            Instant.ofEpochMilli(restingOrder.orderAtEpochMillis()));
        if (restingOrder.filledQuantity() > 0) {
            entry.addFilled(restingOrder.filledQuantity());
        }
        if (restingOrder.cancelled()) {
            entry.cancel();
        }
        return entry;
    }
}
