package com.flab.stocktradingengine.market.mapper;

import java.math.BigDecimal;

import com.flab.stocktradingengine.market.entity.Stock;
import com.flab.stocktradingengine.market.view.StockSearchView;

/**
 * Stock + 현재가 → StockSearchView 변환.
 */
public final class StockSearchViewMapper {

    private StockSearchViewMapper() {
    }

    /**
     * 종목 엔티티와 현재가를 받아 검색 결과 뷰로 변환.
     */
    public static StockSearchView toView(Stock stock, BigDecimal currentPrice) {
        return new StockSearchView(
            stock.getStockCode(),
            stock.getStockName(),
            currentPrice != null ? currentPrice : BigDecimal.ZERO
        );
    }
}
