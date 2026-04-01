package com.flab.stocktradingengine.api.dto.order;

/**
 * 주문·취소 접수 응답 (202 Accepted).
 *
 * <p>주문 접수만 확인. 체결 여부는 별도 조회 API 로 확인한다.</p>
 */
public record OrderAcceptedResponse(String message) {}
