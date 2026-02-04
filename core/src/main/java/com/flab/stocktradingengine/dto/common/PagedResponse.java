package com.flab.stocktradingengine.dto.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;

import java.util.List;

/**
 * 페이지네이션을 포함한 성공 응답
 * 
 * @param <T> 리스트 아이템의 타입
 */
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class PagedResponse<T> implements BaseResponse {
    private final List<T> data;
    private final PaginationInfo pagination;
    private final String code;
    private final String message;
    private final Long timestamp;
    private final boolean success = true;

    private PagedResponse(List<T> data, PaginationInfo pagination, String code, String message, Long timestamp) {
        this.data = data;
        this.pagination = pagination;
        this.code = code;
        this.message = message;
        this.timestamp = timestamp;
    }

    public static <T> PagedResponse<T> of(List<T> data, PaginationInfo pagination) {
        return new PagedResponse<>(data, pagination, "SUCCESS", null, System.currentTimeMillis());
    }

    public static <T> PagedResponse<T> of(List<T> data, PaginationInfo pagination, String message) {
        return new PagedResponse<>(data, pagination, "SUCCESS", message, System.currentTimeMillis());
    }

    /**
     * 페이지네이션 정보를 자동 계산하여 생성
     */
    public static <T> PagedResponse<T> of(
            List<T> data,
            Integer page,
            Integer size,
            Long totalElements,
            String message) {
        Integer totalPages = size > 0 ? (int) ((totalElements + size - 1) / size) : 0;
        PaginationInfo pagination = PaginationInfo.builder()
                .page(page)
                .size(size)
                .totalElements(totalElements)
                .totalPages(totalPages)
                .hasNext(page < totalPages - 1)
                .hasPrevious(page > 0)
                .build();
        return new PagedResponse<>(data, pagination, "SUCCESS", message, System.currentTimeMillis());
    }

    /**
     * 페이지네이션 정보를 자동 계산하여 생성 (메시지 없음)
     */
    public static <T> PagedResponse<T> of(
            List<T> data,
            Integer page,
            Integer size,
            Long totalElements) {
        return of(data, page, size, totalElements, null);
    }
}
