package com.flab.stocktradingengine.dto.order;

import java.math.BigDecimal;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Builder;

/**
 * 매수 주문 요청 DTO
 */
@Builder
public record BuyOrderRequest(
    @NotNull(message = "계좌 ID는 필수입니다")
    Long accountId,

    @NotBlank(message = "종목 코드는 필수입니다")
    String stockCode,

    @NotNull(message = "주문 유형은 필수입니다")
    String orderType, // LIMIT | MARKET

    BigDecimal price, // 지정가인 경우 필수

    @NotNull(message = "수량은 필수입니다")
    @Positive(message = "수량은 양수여야 합니다")
    Integer quantity
) {
}
