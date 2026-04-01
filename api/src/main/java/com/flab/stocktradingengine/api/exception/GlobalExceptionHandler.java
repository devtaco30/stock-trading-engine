package com.flab.stocktradingengine.api.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import com.flab.stocktradingengine.dto.common.ErrorResponse;
import com.flab.stocktradingengine.exception.BusinessException;

import lombok.extern.slf4j.Slf4j;

/**
 * 전역 예외 처리 Handler
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    /**
     * 비즈니스 예외 — BusinessException 하위 클래스 전체 처리.
     * 예외가 들고 있는 httpStatus를 그대로 응답 코드로 사용한다.
     */
    @ExceptionHandler(BusinessException.class)
    public ResponseEntity<ErrorResponse> handleBusinessException(BusinessException e) {
        return ResponseEntity
            .status(e.getHttpStatus())
            .body(ErrorResponse.of(e.getErrorCode(), e.getMessage()));
    }

    /**
     * 예측하지 못한 예외 (최후의 수단)
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleException(Exception e) {
        log.error("처리되지 않은 예외", e);
        String message = e.getMessage() != null && !e.getMessage().isBlank()
            ? "서버 내부 오류가 발생했습니다: " + e.getMessage()
            : "서버 내부 오류가 발생했습니다";
        return ResponseEntity
            .status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(ErrorResponse.of("INTERNAL_ERROR", message));
    }
}
