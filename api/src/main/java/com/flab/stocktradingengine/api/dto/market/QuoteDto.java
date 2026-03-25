package com.flab.stocktradingengine.api.dto.market;

import java.math.BigDecimal;

import com.flab.stocktradingengine.market.view.QuoteView;

import lombok.Builder;

/**
 * 현재가 조회 응답 DTO
 */
@Builder
public record QuoteDto(
    String stockCode,
    String stockName,
    BigDecimal currentPrice,
    BigDecimal previousClose,
    BigDecimal changeRate,
    BigDecimal open,
    BigDecimal high,
    BigDecimal low,
    Long volume
) {

    public static QuoteDto from(QuoteView view) {
        return new QuoteDto(
            view.stockCode(),
            view.stockName(),
            view.currentPrice(),
            view.previousClose(),
            view.changeRate(),
            view.open(),
            view.high(),
            view.low(),
            view.volume()
        );
    }
}
