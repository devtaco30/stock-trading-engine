package com.flab.stocktradingengine.api.dto.account;

import lombok.Builder;

import java.math.BigDecimal;

/**
 * 계좌 목록 조회 응답 DTO
 */
@Builder
public record AccountDto(
    Long accountId,
    BigDecimal balance,
    BigDecimal marginRate, // 증거금률 비율 (0.40, 1.00)
    String status // ACTIVE | IN_ARREARS | FROZEN
) {
}
