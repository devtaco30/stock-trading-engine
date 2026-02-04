package com.flab.stocktradingengine.dto.account;

import lombok.Builder;

import java.math.BigDecimal;

/**
 * 계좌 목록 조회 응답 DTO
 */
@Builder
public record AccountDto(
    String accountId,
    BigDecimal balance,
    BigDecimal withdrawableBalance,
    Integer marginRate,
    String status // ACTIVE | IN_ARREARS | FROZEN
) {
}
