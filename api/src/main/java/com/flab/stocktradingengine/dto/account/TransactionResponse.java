package com.flab.stocktradingengine.dto.account;

import lombok.Builder;

import java.math.BigDecimal;

/**
 * 입출금 응답 DTO
 */
@Builder
public record TransactionResponse(
    String transactionId,
    BigDecimal balance,
    Long transactionAt
) {
}
