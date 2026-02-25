package com.flab.stocktradingengine.dto.market;

import java.math.BigDecimal;

import com.flab.stocktradingengine.market.view.StockSearchView;

import lombok.Builder;

/**
 * 종목 검색 응답 DTO
 */
@Builder
public record StockSearchDto(
    String stockCode,
    String stockName,
    BigDecimal currentPrice
) {

    public static StockSearchDto from(StockSearchView view) {
        return new StockSearchDto(view.stockCode(), view.stockName(), view.currentPrice());
    }
}
