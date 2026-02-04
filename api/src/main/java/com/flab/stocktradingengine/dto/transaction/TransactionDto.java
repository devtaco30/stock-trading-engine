package com.flab.stocktradingengine.dto.transaction;

import lombok.Builder;

import java.math.BigDecimal;

/**
 * 거래 내역 조회 응답 DTO
 */
@Builder
public record TransactionDto(
    Long transactionAt,
    String stockCode,
    String stockName,
    String side, // BUY | SELL
    Integer quantity,
    BigDecimal executionPrice,
    BigDecimal transactionAmount
) {
}
