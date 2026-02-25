package com.flab.stocktradingengine.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import com.flab.stocktradingengine.dto.common.ErrorResponse;

import lombok.extern.slf4j.Slf4j;

/**
 * 전역 예외 처리 Handler
 *
 * <p>모든 컨트롤러에서 발생하는 예외를 일관된 형식으로 처리합니다.
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    /**
     * 인증 관련 예외 처리
     */
    @ExceptionHandler(AuthenticationException.class)
    public ResponseEntity<ErrorResponse> handleAuthenticationException(AuthenticationException e) {
        ErrorResponse errorResponse = ErrorResponse.of(e.getErrorCode(), e.getMessage());
        return ResponseEntity.status(e.getHttpStatus()).body(errorResponse);
    }

    /**
     * 권한 없음 (403)
     */
    @ExceptionHandler(ForbiddenException.class)
    public ResponseEntity<ErrorResponse> handleForbiddenException(ForbiddenException e) {
        ErrorResponse errorResponse = ErrorResponse.of(e.getErrorCode(), e.getMessage());
        return ResponseEntity.status(e.getHttpStatus()).body(errorResponse);
    }

    /**
     * 일반적인 런타임 예외 처리
     */
    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<ErrorResponse> handleRuntimeException(RuntimeException e) {
        log.error("RuntimeException 처리", e);
        String message = e.getMessage() != null && !e.getMessage().isBlank()
            ? "서버 내부 오류가 발생했습니다: " + e.getMessage()
            : "서버 내부 오류가 발생했습니다";
        ErrorResponse errorResponse = ErrorResponse.of("INTERNAL_ERROR", message);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
    }

    /**
     * 모든 예외 처리 (최후의 수단)
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleException(Exception e) {
        log.error("Exception 처리", e);
        String message = e.getMessage() != null && !e.getMessage().isBlank()
            ? "서버 내부 오류가 발생했습니다: " + e.getMessage()
            : "서버 내부 오류가 발생했습니다";
        ErrorResponse errorResponse = ErrorResponse.of("INTERNAL_ERROR", message);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
    }
}
