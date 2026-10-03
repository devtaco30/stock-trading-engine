package com.flab.stocktradingengine.api.messaging;

/**
 * 전달에 끝내 실패한 주문의 기록 장소. 실패한 경로에서만 불린다.
 */
public interface OrderDeliveryFailureStore {

    /**
     * @param reason 마지막 실패 사유
     * @param attempts 전달을 시도한 횟수(종료 시 큐에 남은 주문은 0)
     * @param failedAtEpochMillis 실패로 확정한 시각
     */
    void put(PendingOrder order, String reason, int attempts, long failedAtEpochMillis);
}
