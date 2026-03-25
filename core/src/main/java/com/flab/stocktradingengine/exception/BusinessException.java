package com.flab.stocktradingengine.exception;

/**
 * 비즈니스 규칙 위반 예외의 공통 기반 클래스.
 * 도메인·API 계층에서 발생하는 모든 예측 가능한 오류는 이 클래스를 상속한다.
 * GlobalExceptionHandler가 httpStatus를 그대로 응답 코드로 사용한다.
 */
public class BusinessException extends RuntimeException {

    private final String errorCode;
    private final int httpStatus;

    public BusinessException(String errorCode, String message, int httpStatus) {
        super(message);
        this.errorCode = errorCode;
        this.httpStatus = httpStatus;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public int getHttpStatus() {
        return httpStatus;
    }
}
