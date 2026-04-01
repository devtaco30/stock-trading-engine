package com.flab.stocktradingengine.user.exception;

import com.flab.stocktradingengine.exception.BusinessException;

/**
 * 토큰이 유효하지 않을 때 (401) — 형식 오류, 만료, 서명 불일치 등.
 */
public class InvalidTokenException extends BusinessException {

    public InvalidTokenException(String message) {
        super("INVALID_TOKEN", message, 401);
    }
}
