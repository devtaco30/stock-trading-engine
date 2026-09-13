package com.flab.stocktradingengine.account.disruptor.domain;

import java.math.BigDecimal;

/**
 * 검증·예약 결과(불변 값).
 * <p>accepted 면 reservedMargin 에 이번 주문의 예약 증거금이 담기고 reason 은 null.
 * rejected 면 reservedMargin 은 null, reason 에 거부 사유가 담긴다.</p>
 */
public record ReserveResult(boolean accepted, BigDecimal reservedMargin, RejectReason reason) {

    public static ReserveResult accepted(BigDecimal reservedMargin) {
        return new ReserveResult(true, reservedMargin, null);
    }

    public static ReserveResult rejected(RejectReason reason) {
        return new ReserveResult(false, null, reason);
    }
}
