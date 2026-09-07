package com.flab.stocktradingengine.account.disruptor;

import java.math.BigDecimal;

/**
 * 링버퍼 슬롯(가변) — 매수 검증·예약 명령과 체결 반영 명령을 담는다.
 *
 * <p>matching-disruptor 의 {@code OrderEvent} 와 같은 이유로 가변 컨테이너다.
 * 링버퍼가 슬롯을 미리 만들어 재사용하므로, 발행마다 새 객체를 만들지 않는다.
 * {@link EventType} 에 따라 BUY(검증·예약) / BUY_FILL·SELL_FILL(체결 반영) 셋을 구분한다.</p>
 */
public class AccountEvent {

    private EventType type;
    private long orderId;
    private long accountId;
    private String stockCode;
    private BigDecimal price;
    private int quantity;
    private String requestId;
    private long tradeId;

    /** 매수 검증·예약 명령으로 슬롯을 채운다. orderId 는 접수 시 부여된 주문 신원(장부 키). */
    public void setBuy(long orderId, long accountId, String stockCode, BigDecimal price, int quantity, String requestId) {
        this.type = EventType.BUY;
        this.orderId = orderId;
        this.accountId = accountId;
        this.stockCode = stockCode;
        this.price = price;
        this.quantity = quantity;
        this.requestId = requestId;
    }

    /** 매수 체결 반영 명령으로 슬롯을 채운다. tradeId 는 체결 신원(멱등키). */
    public void setBuyFill(long tradeId, long orderId, long accountId, String stockCode, BigDecimal matchPrice, int quantity) {
        this.type = EventType.BUY_FILL;
        this.tradeId = tradeId;
        this.orderId = orderId;
        this.accountId = accountId;
        this.stockCode = stockCode;
        this.price = matchPrice;
        this.quantity = quantity;
    }

    /** 매도 체결 반영 명령으로 슬롯을 채운다. tradeId 는 체결 신원(멱등키). */
    public void setSellFill(long tradeId, long orderId, long accountId, String stockCode, int quantity) {
        this.type = EventType.SELL_FILL;
        this.tradeId = tradeId;
        this.orderId = orderId;
        this.accountId = accountId;
        this.stockCode = stockCode;
        this.quantity = quantity;
    }

    /** 소비 직후 참조 필드를 비워 이전 명령을 붙들지 않게 한다. */
    public void clear() {
        this.type = null;
        this.orderId = 0L;
        this.accountId = 0L;
        this.stockCode = null;
        this.price = null;
        this.quantity = 0;
        this.requestId = null;
        this.tradeId = 0L;
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

    public BigDecimal getPrice() {
        return price;
    }

    public int getQuantity() {
        return quantity;
    }

    public String getRequestId() {
        return requestId;
    }

    public long getTradeId() {
        return tradeId;
    }
}
