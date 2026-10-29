package com.flab.stocktradingengine.matching.worker.coordination;

/** 이동으로 받는 종목 slot의 호가창을 이전 담당에게 요청하는 쪽. 배정 콜백이 부르므로 오래 걸리지 않고 돌아와야 한다. */
@FunctionalInterface
public interface MovedSlotRequests {

    void requestHandover(int stockSlot);

    /** 진행 중이던 요청을 취소한다. 그 요청의 응답은 버려진다. 이동으로 받는 도중 종목 slot이 회수되었을 때 부른다. */
    default void cancelHandover(int stockSlot) {
    }
}
