package com.flab.stocktradingengine.api.mapper;

import java.math.BigDecimal;
import java.math.RoundingMode;

import com.flab.stocktradingengine.account.entity.Holding;
import com.flab.stocktradingengine.api.dto.account.HoldingDto;
import com.flab.stocktradingengine.market.view.StockInfo;

/**
 * Holding 엔티티 + 보강 데이터(종목명, 현재가) → HoldingDto 변환.
 */
public final class HoldingDtoMapper {

    private HoldingDtoMapper() {
    }

    /**
     * 보유 엔티티와 보강 데이터를 받아 DTO로 변환.
     */
    public static HoldingDto toHoldingDto(Holding holding, StockInfo stockInfo) {
        BigDecimal currentPrice = stockInfo.currentPrice();
        BigDecimal marketValue = currentPrice.multiply(BigDecimal.valueOf(holding.getQuantity()));
        BigDecimal acquisitionAmount = holding.getAveragePrice().multiply(BigDecimal.valueOf(holding.getQuantity()));
        BigDecimal pnl = marketValue.subtract(acquisitionAmount);
        BigDecimal profitRate = acquisitionAmount.compareTo(BigDecimal.ZERO) != 0
            ? pnl.divide(acquisitionAmount, 4, RoundingMode.HALF_UP).multiply(BigDecimal.valueOf(100))
            : BigDecimal.ZERO;
        return HoldingDto.builder()
            .stockCode(holding.getStockCode())
            .stockName(stockInfo.stockName())
            .quantity(holding.getQuantity())
            .averagePrice(holding.getAveragePrice())
            .currentPrice(currentPrice)
            .evaluationAmount(marketValue)
            .profit(pnl)
            .profitRate(profitRate)
            .build();
    }
}
