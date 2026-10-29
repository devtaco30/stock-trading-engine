package com.flab.stocktradingengine.matching.worker.u10;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import com.flab.stocktradingengine.matching.disruptor.snapshot.BookSnapshot;
import com.flab.stocktradingengine.matching.disruptor.snapshot.RestingOrder;
import com.flab.stocktradingengine.matching.worker.recovery.MatchingSnapshotStore;
import com.flab.stocktradingengine.matching.worker.recovery.StoredMatchingSnapshot;

/**
 * U10 확인 스크립트용 읽기 도구 — 매칭 워커가 정상 종료할 때 archive-dir에 쓴 스냅샷을 읽어
 * 맡은 종목 slot과 호가창의 대기 주문을 줄 단위로 출력한다. 스냅샷이 없으면 종료 코드 2다.
 *
 * <pre>
 * OWNED count=32 min=0 max=31
 * BOOK stock=000001 resting=1 orderIds=123
 * </pre>
 */
public final class U10MatchingSnapshotDump {

    private U10MatchingSnapshotDump() {
    }

    public static void main(String[] args) {
        MatchingSnapshotStore store = new MatchingSnapshotStore(new File(args[0]));
        Optional<StoredMatchingSnapshot> stored = store.read();
        if (stored.isEmpty()) {
            System.out.println("NO_SNAPSHOT");
            System.exit(2);
        }
        StoredMatchingSnapshot snapshot = stored.get();
        System.out.println("OWNED count=" + snapshot.ownedStockSlots().size()
            + " min=" + (snapshot.ownedStockSlots().isEmpty() ? -1 : Collections.min(snapshot.ownedStockSlots()))
            + " max=" + (snapshot.ownedStockSlots().isEmpty() ? -1 : Collections.max(snapshot.ownedStockSlots())));
        Map<String, BookSnapshot> books = new TreeMap<>(snapshot.snapshot().booksByStock());
        for (Map.Entry<String, BookSnapshot> entry : books.entrySet()) {
            StringBuilder orderIds = new StringBuilder();
            for (RestingOrder order : entry.getValue().restingOrders()) {
                if (orderIds.length() > 0) {
                    orderIds.append(',');
                }
                orderIds.append(order.orderId());
            }
            System.out.println("BOOK stock=" + entry.getKey()
                + " resting=" + entry.getValue().restingOrders().size() + " orderIds=" + orderIds);
        }
    }
}
