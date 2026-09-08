package com.flab.stocktradingengine.matching.disruptor;

import java.math.BigDecimal;
import java.time.Instant;

import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.wire.EventType;

/**
 * 링버퍼의 슬롯 객체 (가변).
 *
 * <p><b>왜 가변인가</b><br>
 * Disruptor 는 링버퍼 생성 시 모든 슬롯을 미리 만들어 두고 재사용한다.
 * 매 발행마다 새 객체를 만들지 않으므로 GC 부담이 줄어든다.
 * 그래서 이 객체는 불변 record 가 아니라, 발행할 때마다 필드를 덮어쓰는 가변 컨테이너다.</p>
 *
 * <p><b>스레드 안전성</b><br>
 * 프로듀서가 {@code ringBuffer.next()} 로 예약한 슬롯에만 쓰고,
 * 소비자는 {@code publish} 된 슬롯만 읽는다. 링버퍼의 sequence 가 이 순서를 보장하므로
 * 슬롯 자체에 동기화는 두지 않는다.</p>
 */
public class OrderEvent {

    private EventType type;
    private long orderId;
    private long accountId;
    private String stockCode;
    private OrderSide side;
    private BigDecimal price;
    private int quantity;
    private Instant orderAt;

    /** 주문 접수 명령으로 슬롯을 채운다. */
    public void setPlace(long orderId, long accountId, String stockCode,
                         OrderSide side, BigDecimal price, int quantity, Instant orderAt) {
        this.type = EventType.PLACE;
        this.orderId = orderId;
        this.accountId = accountId;
        this.stockCode = stockCode;
        this.side = side;
        this.price = price;
        this.quantity = quantity;
        this.orderAt = orderAt;
    }

    /** 주문 취소 명령으로 슬롯을 채운다. 취소는 orderId·stockCode 만 필요하다. */
    public void setCancel(long orderId, String stockCode) {
        this.type = EventType.CANCEL;
        this.orderId = orderId;
        this.stockCode = stockCode;
    }

    /**
     * 슬롯을 비운다. 소비 직후 호출해 참조 필드(String·BigDecimal·Instant 등)를 null 로 되돌린다.
     * 다음 재사용 전까지 이전 주문 데이터를 붙들고 있지 않도록 해 불필요한 메모리 보유를 막는다.
     */
    public void clear() {
        this.type = null;
        this.orderId = 0L;
        this.accountId = 0L;
        this.stockCode = null;
        this.side = null;
        this.price = null;
        this.quantity = 0;
        this.orderAt = null;
    }

    public EventType getType() {
        return type;
    }

    public long getOrderId() {
        return orderId;
    }

    public long getAccountId() {
        return accountId;
    }

    public String getStockCode() {
        return stockCode;
    }

    public OrderSide getSide() {
        return side;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public int getQuantity() {
        return quantity;
    }

    public Instant getOrderAt() {
        return orderAt;
    }
}
