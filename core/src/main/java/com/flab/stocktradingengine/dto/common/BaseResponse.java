package com.flab.stocktradingengine.dto.common;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * API 응답의 최상위 인터페이스
 * 
 * 모든 API 응답이 구현해야 하는 기본 구조를 정의합니다.
 * sealed interface를 사용하여 타입 안전성을 보장하고,
 * 성공/실패 응답을 명확히 구분합니다.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public sealed interface BaseResponse permits ApiResponse, ErrorResponse, PagedResponse {
    // Lombok @Getter로 자동 생성되므로 명시적 메서드 선언 불필요
}
