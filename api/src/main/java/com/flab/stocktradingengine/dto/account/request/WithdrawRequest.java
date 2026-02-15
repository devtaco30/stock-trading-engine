package com.flab.stocktradingengine.dto.account.request;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Builder;

import java.math.BigDecimal;

/**
 * 출금 요청 DTO
 */
@Builder
public record WithdrawRequest(
    @NotNull(message = "출금액은 필수입니다")
    @Positive(message = "출금액은 양수여야 합니다")
    BigDecimal amount
) {
}
