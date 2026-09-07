package com.flab.stocktradingengine.matching.disruptor;

import java.math.BigDecimal;
import java.time.Instant;

import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * 저널에 남기는 주문 스냅샷 (불변).
 *
 * <p><b>왜 OrderEvent 를 그대로 저장하지 않는가</b><br>
 * {@link OrderEvent} 는 링버퍼가 재사용하는 가변 슬롯이라, 다음 발행 때 필드가 덮어써진다.
 * 그 참조를 그대로 저널에 담으면 나중에 값이 바뀌어 기록이 훼손된다.
 * 그래서 저널러는 발행 시점의 값을 이 불변 record 로 복사해 담는다.</p>
 */
public record JournaledOrder(
    EventType type,
    long orderId,
    long accountId,
    String stockCode,
    OrderSide side,
    BigDecimal price,
    int quantity,
    Instant orderAt
) {
    /** 링버퍼 슬롯의 현재 값을 불변 스냅샷으로 복사한다. */
    public static JournaledOrder from(OrderEvent event) {
        return new JournaledOrder(
            event.getType(),
            event.getOrderId(),
            event.getAccountId(),
            event.getStockCode(),
            event.getSide(),
            event.getPrice(),
            event.getQuantity(),
            event.getOrderAt()
        );
    }
}
