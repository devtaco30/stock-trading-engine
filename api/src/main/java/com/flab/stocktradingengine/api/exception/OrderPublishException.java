package com.flab.stocktradingengine.api.exception;

import com.flab.stocktradingengine.exception.BusinessException;

/**
 * 계좌 인테이크로의 Aeron 주문 발신이 재시도 끝에도 실패했을 때(503).
 *
 * <p>주문을 조용히 버리지 않는다(돈) — 클라이언트가 같은 requestId로 재전송하면 계좌 엔진의
 * 멱등이 중복을 막아준다(ADR-032, fork5 U1b).</p>
 */
public class OrderPublishException extends BusinessException {

    private static final int HTTP_STATUS_SERVICE_UNAVAILABLE = 503;

    public OrderPublishException(String message) {
        super("ORDER_PUBLISH_FAILED", message, HTTP_STATUS_SERVICE_UNAVAILABLE);
    }
}
