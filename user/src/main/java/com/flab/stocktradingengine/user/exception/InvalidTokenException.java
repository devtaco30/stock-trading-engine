package com.flab.stocktradingengine.user.exception;

/**
 * 토큰이 유효하지 않을 때 (형식 오류, 만료, 서명 불일치 등)
 */
public class InvalidTokenException extends RuntimeException {

	public InvalidTokenException(String message) {
		super(message);
	}

	public InvalidTokenException(String message, Throwable cause) {
		super(message, cause);
	}
}
