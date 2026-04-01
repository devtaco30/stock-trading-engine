package com.flab.stocktradingengine.api.dto.account;

import lombok.Builder;

import java.math.BigDecimal;

/**
 * 보유 종목 조회 응답 DTO
 */
@Builder
public record HoldingDto(
    String stockCode,
    String stockName,
    Integer quantity,
    BigDecimal averagePrice,
    BigDecimal currentPrice,
    BigDecimal evaluationAmount,
    BigDecimal profit,
    BigDecimal profitRate
) {
}
