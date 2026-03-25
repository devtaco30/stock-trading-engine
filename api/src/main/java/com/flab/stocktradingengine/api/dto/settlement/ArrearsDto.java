package com.flab.stocktradingengine.api.dto.settlement;

import lombok.Builder;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * 미수금 조회 응답 DTO
 */
@Builder
public record ArrearsDto(
    String arrearsId,
    BigDecimal amount,
    LocalDate occurredDate,
    Integer overdueDays,
    BigDecimal accumulatedInterest
) {
}
