package com.flab.stocktradingengine.account.disruptor;

/**
 * 매도 예약 장부 한 줄의 스냅샷(2d-2). {@code AccountState.SellReservation}은 private record라
 * 엔진 밖으로 노출되지 않으므로, 스냅샷이 볼 수 있는 형태로 따로 둔다.
 */
public record SellReservationSnapshot(String stockCode, int remainingQuantity) {
}
