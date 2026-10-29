package com.flab.stocktradingengine.matching.worker.handover;

import java.util.List;

/** 조립을 마친 호가창 이동을 받아 엔진에 적재를 요청하는 쪽. 실제 구현은 {@code SlotEventRequests#requestLoad}다. */
@FunctionalInterface
public interface HandoverLoadSink {

    void onAssembled(long transferId, int stockSlot, List<byte[]> bookBytesPerStock);
}
