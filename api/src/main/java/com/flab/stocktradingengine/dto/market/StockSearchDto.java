package com.flab.stocktradingengine.dto.market;

import lombok.Builder;

import java.math.BigDecimal;

/**
 * 종목 검색 응답 DTO
 */
@Builder
public record StockSearchDto(
    String stockCode,
    String stockName,
    BigDecimal currentPrice
) {
}
