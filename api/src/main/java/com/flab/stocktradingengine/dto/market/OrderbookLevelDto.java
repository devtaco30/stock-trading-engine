package com.flab.stocktradingengine.dto.market;

import java.math.BigDecimal;

import lombok.Builder;

/**
 * 호가창 한 단계 (가격·수량).
 */
@Builder
public record OrderbookLevelDto(
    BigDecimal price,
    int quantity
) {}
