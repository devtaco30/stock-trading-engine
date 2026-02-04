package com.flab.stocktradingengine.dto.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;

/**
 * ApiResponse 응답을 위한 데이터 클래스
 * 
 * @param <T> 실제 응답 데이터의 타입
 */
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class ApiResponse<T> implements BaseResponse {
    private final T data;
    private final String code;
    private final String message;
    private final Long timestamp;
    private final boolean success = true;

    private ApiResponse(T data, String code, String message, Long timestamp) {
        this.data = data;
        this.code = code;
        this.message = message;
        this.timestamp = timestamp;
    }

    public static <T> ApiResponse<T> of(T data) {
        return new ApiResponse<>(data, "SUCCESS", null, System.currentTimeMillis());
    }

    public static <T> ApiResponse<T> of(T data, String message) {
        return new ApiResponse<>(data, "SUCCESS", message, System.currentTimeMillis());
    }

    public static <T> ApiResponse<T> of(T data, String code, String message) {
        return new ApiResponse<>(data, code, message, System.currentTimeMillis());
    }
}
