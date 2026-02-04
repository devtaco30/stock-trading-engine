package com.flab.stocktradingengine.dto.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;

/**
 * 실패 응답을 위한 데이터 클래스
 */
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class ErrorResponse implements BaseResponse {
    private final String code;
    private final String message;
    private final Object details;
    private final Long timestamp;
    private final boolean success = false;

    private ErrorResponse(String code, String message, Object details, Long timestamp) {
        this.code = code;
        this.message = message;
        this.details = details;
        this.timestamp = timestamp;
    }

    public static ErrorResponse of(String code, String message) {
        return new ErrorResponse(code, message, null, System.currentTimeMillis());
    }

    public static ErrorResponse of(String code, String message, Object details) {
        return new ErrorResponse(code, message, details, System.currentTimeMillis());
    }
}
