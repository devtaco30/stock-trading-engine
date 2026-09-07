package com.flab.stocktradingengine.account.disruptor;

/**
 * 매도 보유예약 결과(불변 값).
 * <p>accepted 면 reservedQuantity 에 이번 주문에 예약된 수량이 담기고 reason 은 null.
 * rejected 면 reservedQuantity 는 0, reason 에 거부 사유가 담긴다.</p>
 *
 * <p>{@link ReserveResult}(매수)와 별도 타입인 이유: 매도 예약은 증거금(금액) 개념이 없어
 * 같은 타입을 쓰면 필드 의미가 헷갈린다.</p>
 */
public record SellReserveResult(boolean accepted, int reservedQuantity, RejectReason reason) {

    public static SellReserveResult accepted(int reservedQuantity) {
        return new SellReserveResult(true, reservedQuantity, null);
    }

    public static SellReserveResult rejected(RejectReason reason) {
        return new SellReserveResult(false, 0, reason);
    }
}
