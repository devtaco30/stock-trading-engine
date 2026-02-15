package com.flab.stocktradingengine.dto.account.response;

import java.math.BigDecimal;

import lombok.Builder;

/**
 * 입출금 응답 DTO.
 * <p>Transaction = 금융권 용어로 입·출금 1건(계좌 잔액 변동).</p>
 */
@Builder
public record TransactionResponse(
    BigDecimal balance,
    Long respondedAt
) {
}
