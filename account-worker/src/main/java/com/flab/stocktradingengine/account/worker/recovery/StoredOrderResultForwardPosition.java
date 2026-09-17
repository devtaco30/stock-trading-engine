package com.flab.stocktradingengine.account.worker.recovery;

/** {@link OrderResultForwardPositionStore}가 읽어 돌려주는 값. */
public record StoredOrderResultForwardPosition(long recordingId, long position) {
}
