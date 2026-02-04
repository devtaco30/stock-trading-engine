package com.flab.stocktradingengine.dto.common;

import lombok.Builder;

/**
 * 페이지네이션 정보
 */
@Builder
public record PaginationInfo(
    Integer page,              // 현재 페이지 번호 (0부터 시작)
    Integer size,              // 페이지 크기
    Long totalElements,        // 전체 요소 수
    Integer totalPages,        // 전체 페이지 수
    Boolean hasNext,           // 다음 페이지 존재 여부
    Boolean hasPrevious        // 이전 페이지 존재 여부
) {
}
