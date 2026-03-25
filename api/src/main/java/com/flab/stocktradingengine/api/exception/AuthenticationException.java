package com.flab.stocktradingengine.api.exception;

import com.flab.stocktradingengine.exception.BusinessException;

/**
 * 인증 관련 예외 (401).
 * 인증이 필요하거나 인증에 실패한 경우 발생한다.
 */
public class AuthenticationException extends BusinessException {

    public AuthenticationException(String errorCode, String message) {
        super(errorCode, message, 401);
    }

    public static AuthenticationException authRequired() {
        return new AuthenticationException("AUTH_REQUIRED", "인증이 필요합니다");
    }

    public static AuthenticationException invalidTokenFormat(String message) {
        return new AuthenticationException("INVALID_TOKEN_FORMAT", message);
    }
}
