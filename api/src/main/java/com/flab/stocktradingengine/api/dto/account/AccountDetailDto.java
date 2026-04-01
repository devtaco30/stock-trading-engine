package com.flab.stocktradingengine.api.dto.account;

import lombok.Builder;

import java.math.BigDecimal;

/**
 * 계좌 상세 조회 응답 DTO
 */
@Builder
public record AccountDetailDto(
    Long accountId,
    BigDecimal balance,
    BigDecimal withdrawableBalance, // 출금가능 잔액
    BigDecimal totalAssets, // 총 자산
    BigDecimal unpaidAmount, // 미결제 금액
    BigDecimal pendingSellAmount, // 매도 예정 금액
    BigDecimal buyLimit, // 매수 가능 금액
    BigDecimal marginRate, // 증거금률 비율 (0.40, 1.00)
    String status // ACTIVE | IN_ARREARS | FROZEN
) {
}
