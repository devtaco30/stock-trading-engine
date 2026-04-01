package com.flab.stocktradingengine.api.dto.market;

import java.util.List;

import lombok.Builder;

/**
 * 종목 호가창 응답 DTO.
 * <p>bids: 매수 호가 (가격 내림차순), asks: 매도 호가 (가격 오름차순).</p>
 */
@Builder
public record OrderbookDto(
    String stockCode,
    List<OrderbookLevelDto> bids,
    List<OrderbookLevelDto> asks
) {}
