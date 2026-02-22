package com.flab.stocktradingengine.dto.account;

import java.math.BigDecimal;

/**
 * 계좌 상세 조회 시 퍼사드가 수집한 읽기 전용 데이터.
 * <p>증거금 합계, 미결제 합계, 보유 평가금 합계.</p>
 */
public record AccountDetailData(
    BigDecimal reservedMarginSum,
    BigDecimal unpaidSum,
    BigDecimal totalAssetsFromHoldings
) {
}
