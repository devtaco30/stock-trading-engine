package com.flab.stocktradingengine.dto.settlement;

import lombok.Builder;

import java.math.BigDecimal;

/**
 * 미수금 조기 변제 응답 DTO
 */
@Builder
public record RepaymentResponse(
    BigDecimal remainingAmount,
    Long repaymentAt
) {
}
