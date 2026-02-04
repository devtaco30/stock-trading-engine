package com.flab.stocktradingengine.dto.market;

import lombok.Builder;

import java.math.BigDecimal;

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
}
