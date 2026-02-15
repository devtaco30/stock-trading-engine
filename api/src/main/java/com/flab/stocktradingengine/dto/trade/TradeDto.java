package com.flab.stocktradingengine.dto.trade;

import java.math.BigDecimal;

import lombok.Builder;

/**
 * 체결(매매) 1건 응답 DTO.
 * <p>Trade = 증권 업계에서 체결된 매수/매도 1건.</p>
 */
@Builder
public record TradeDto(
    Long filledAt,
    String stockCode,
    String stockName,
    String side, // BUY | SELL
    Integer quantity,
    BigDecimal executionPrice,
    BigDecimal amount
) {
}
