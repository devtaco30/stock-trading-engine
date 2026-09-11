package com.flab.stocktradingengine.matching.disruptor;

import java.math.BigDecimal;

import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * 스냅샷(2d-1)이 찍는 미체결 주문 하나의 불변 스냅샷. {@code trading.matching.OrderEntry}(가변,
 * matching 코어 전용 인메모리 표현)를 그대로 저널·파일에 담으면 이후 체결로 필드가 바뀌어 스냅샷이
 * 훼손되므로, 찍은 시점의 값을 이 레코드로 복사해 담는다.
 *
 * <p>stockCode는 담지 않는다 — {@link MatchingSnapshot#booksByStock()}이 이미 종목코드로
 * 묶어놓은 자리에 들어가므로 중복이다.</p>
 */
public record RestingOrder(
    long orderId,
    long accountId,
    OrderSide side,
    BigDecimal price,
    int quantity,
    long orderAtEpochMillis,
    int filledQuantity,
    boolean cancelled
) {
}
