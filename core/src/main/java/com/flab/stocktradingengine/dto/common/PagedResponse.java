package com.flab.stocktradingengine.dto.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;

import java.util.List;

/**
 * 페이지네이션을 포함한 목록 응답.
 * <p>공통 메타(code, timestamp, success)는 ApiResponse 한 겹에서만 사용하고, 본 DTO는 data + pagination 만 가진다.</p>
 *
 * @param <T> 리스트 아이템의 타입
 */
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class PagedResponse<T> implements BaseResponse {
    private final List<T> data;
    private final PaginationInfo pagination;

    private PagedResponse(List<T> data, PaginationInfo pagination) {
        this.data = data;
        this.pagination = pagination;
    }

    public static <T> PagedResponse<T> of(List<T> data, PaginationInfo pagination) {
        return new PagedResponse<>(data, pagination);
    }

    public static <T> PagedResponse<T> of(List<T> data, PaginationInfo pagination, String message) {
        return new PagedResponse<>(data, pagination);
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
        return new PagedResponse<>(data, pagination);
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
