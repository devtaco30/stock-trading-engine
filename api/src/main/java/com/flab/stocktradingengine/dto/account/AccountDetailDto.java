package com.flab.stocktradingengine.dto.account;

import lombok.Builder;

import java.math.BigDecimal;

/**
 * 계좌 상세 조회 응답 DTO
 */
@Builder
public record AccountDetailDto(
    String accountId,
    BigDecimal balance,
    BigDecimal withdrawableBalance,
    BigDecimal totalAssets,
    BigDecimal unpaidAmount,
    BigDecimal pendingSellAmount,
    BigDecimal buyLimit,
    Integer marginRate,
    String status // ACTIVE | IN_ARREARS | FROZEN
) {
}
