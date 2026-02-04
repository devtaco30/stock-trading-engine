package com.flab.stocktradingengine.dto.settlement;

import lombok.Builder;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * 미결제 내역 조회 응답 DTO
 */
@Builder
public record UnpaidSettlementDto(
    String settlementId,
    String stockCode,
    LocalDate settlementDate,
    BigDecimal amount,
    String status // PENDING | SETTLED
) {
}
