package com.flab.stocktradingengine.dto.settlement;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Builder;

import java.math.BigDecimal;

/**
 * 미수금 조기 변제 요청 DTO
 */
@Builder
public record RepaymentRequest(
    @NotNull(message = "상환 금액은 필수입니다")
    @Positive(message = "상환 금액은 양수여야 합니다")
    BigDecimal amount
) {
}
