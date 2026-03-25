package com.flab.stocktradingengine.exception;

/**
 * 요청 자체가 유효하지 않을 때 (400).
 * 가격 제한폭 초과, 잘못된 상태 전이, 유효하지 않은 입력값 등에 사용한다.
 */
public class InvalidRequestException extends BusinessException {

    public InvalidRequestException(String message) {
        super("INVALID_REQUEST", message, 400);
    }
}
