package com.flab.stocktradingengine.matching.worker.coordination;

/** 이동으로 받는 종목 slot의 호가창을 이전 담당에게 요청하는 쪽. 배정 콜백이 부르므로 오래 걸리지 않고 돌아와야 한다. */
@FunctionalInterface
public interface MovedSlotRequests {

    void requestHandover(int stockSlot);
}
