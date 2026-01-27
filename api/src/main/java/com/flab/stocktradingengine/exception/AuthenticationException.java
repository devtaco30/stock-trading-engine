package com.flab.stocktradingengine.exception;

/**
 * 인증 관련 예외
 * 
 * <p>인증이 필요하거나 인증에 실패한 경우 발생합니다.
 */
public class AuthenticationException extends RuntimeException {

    private final String errorCode;
    private final int httpStatus;

    public AuthenticationException(String errorCode, String message, int httpStatus) {
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

    public static AuthenticationException authRequired() {
        return new AuthenticationException("AUTH_REQUIRED", "인증이 필요합니다", 401);
    }

    public static AuthenticationException invalidTokenFormat(String message) {
        return new AuthenticationException("INVALID_TOKEN_FORMAT", message, 401);
    }
}
