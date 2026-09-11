package com.flab.stocktradingengine.matching.disruptor;

import java.util.List;
import java.util.Map;

/**
 * 종목 하나의 호가창 스냅샷(2d-1) — 미체결 주문과 전량 체결 멱등 캐시(filledOrderTimestamps)를
 * 같이 담는다. 멱등 캐시는 {@code OrderBook} 인스턴스(=종목)별로 따로 있으므로, orderId만으로
 * 전역 맵을 두면 복원할 때 "어느 종목 호가창에 넣어야 하는지"를 알 수 없다 — 그래서 이 레코드처럼
 * 종목 단위로 같이 묶는다.
 */
public record BookSnapshot(
    List<RestingOrder> restingOrders,
    Map<Long, Long> filledOrderTimestampsEpochMillis
) {
}
