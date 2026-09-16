package com.flab.stocktradingengine.account.disruptor.engine;

import java.math.BigDecimal;

import com.flab.stocktradingengine.codec.AccountEventType;

/**
 * 링버퍼 슬롯(가변) — 매수 검증·예약 명령과 체결 반영 명령을 담는다.
 *
 * <p>matching-disruptor 의 {@code OrderEvent} 와 같은 이유로 가변 컨테이너다.
 * 링버퍼가 슬롯을 미리 만들어 재사용하므로, 발행마다 새 객체를 만들지 않는다.
 * {@link AccountEventType} 에 따라 BUY·SELL(검증·예약) / BUY_FILL·SELL_FILL(체결 반영) 넷을 구분한다.</p>
 */
public class AccountEvent {

    private AccountEventType type;
    private long orderId;
    private long accountId;
    private String stockCode;
    private BigDecimal price;
    private int quantity;
    private String requestId;
    private long tradeId;
    private long sourcePosition;
    private int sourceSessionId;
    private long journaledPosition;
    private long publishedAtEpochNanos;

    /**
     * 매수 검증·예약 명령으로 슬롯을 채운다. orderId 는 아직 없다(C5-2a) — 핸들러가 첫 접수
     * 시점에 직접 발급하므로, 슬롯 재사용으로 이전 명령의 orderId 가 남지 않게 명시적으로 비운다.
     *
     * @param publishedAtEpochNanos 이 주문이 발행된 시각(끝점① 접수 지연 측정용, decision_records/
     *                              v1-v2-e2e-measurement.md). 발행 시각을 모르는 발행자(복구 replay
     *                              등)는 0을 넘긴다 — {@code sourcePosition}과 같은 관례.
     */
    public void setBuy(long accountId, String stockCode, BigDecimal price, int quantity, String requestId, long publishedAtEpochNanos) {
        this.type = AccountEventType.BUY;
        this.orderId = 0L;
        this.accountId = accountId;
        this.stockCode = stockCode;
        this.price = price;
        this.quantity = quantity;
        this.requestId = requestId;
        this.publishedAtEpochNanos = publishedAtEpochNanos;
    }

    /**
     * 매도 검증·예약 명령으로 슬롯을 채운다. price 는 계좌 예약(담보=보유 수량)엔 안 쓰이지만,
     * 매칭 전달용으로 실어 둔다(②-a — 매칭은 지정가 엔진이라 호가창 가격레벨에 필요하다).
     * orderId 는 {@link #setBuy}와 같은 이유로 비운다. publishedAtEpochNanos 도 {@link #setBuy}와 같다.
     */
    public void setSell(long accountId, String stockCode, BigDecimal price, int quantity, String requestId, long publishedAtEpochNanos) {
        this.type = AccountEventType.SELL;
        this.orderId = 0L;
        this.accountId = accountId;
        this.stockCode = stockCode;
        this.price = price;
        this.quantity = quantity;
        this.requestId = requestId;
        this.publishedAtEpochNanos = publishedAtEpochNanos;
    }

    /**
     * 매수 체결 반영 명령으로 슬롯을 채운다. tradeId 는 체결 신원(멱등키). sourcePosition 은 이 체결을
     * 실어 보낸 수신 스트림의 위치(1-2) — 소비자 스레드가 실제로 이 체결을 반영한 뒤에야 그 값을
     * "적용 완료 위치"로 인정한다. 수신 스레드가 도착시킨 위치({@code image.position()})와는 다르다.
     * sourceSessionId 는 그 위치가 어느 발행자(Aeron 연결)의 것인지 구분하는 키(ADR-032 I1, D1) —
     * 매칭 프로세스가 둘 이상이면 recording 이 여럿 생기고, position 값 하나만으로는 어느 recording의
     * 위치인지 구분할 수 없다.
     */
    public void setBuyFill(long tradeId, long orderId, long accountId, String stockCode, BigDecimal matchPrice, int quantity, long sourcePosition, int sourceSessionId) {
        this.type = AccountEventType.BUY_FILL;
        this.tradeId = tradeId;
        this.orderId = orderId;
        this.accountId = accountId;
        this.stockCode = stockCode;
        this.price = matchPrice;
        this.quantity = quantity;
        this.sourcePosition = sourcePosition;
        this.sourceSessionId = sourceSessionId;
    }

    /** 매도 체결 반영 명령으로 슬롯을 채운다. tradeId 는 체결 신원(멱등키). sourcePosition·sourceSessionId 는 {@link #setBuyFill}과 같다. */
    public void setSellFill(long tradeId, long orderId, long accountId, String stockCode, int quantity, long sourcePosition, int sourceSessionId) {
        this.type = AccountEventType.SELL_FILL;
        this.tradeId = tradeId;
        this.orderId = orderId;
        this.accountId = accountId;
        this.stockCode = stockCode;
        this.quantity = quantity;
        this.sourcePosition = sourcePosition;
        this.sourceSessionId = sourceSessionId;
    }

    /**
     * 정산 되돌림 명령으로 슬롯을 채운다. settlementRef 는 정산 신원(멱등키) — tradeId 필드를 그대로
     * 재사용한다(둘 다 "이 EventType에서의 멱등키" 역할이라 필드를 늘리지 않는다). amount 는
     * price 필드를 재사용한다(둘 다 금액이고, BUY_FILL 에서 matchPrice 를 price 필드에 담는 것과 같은 결).
     */
    public void setSettlement(long settlementRef, long accountId, BigDecimal amount) {
        this.type = AccountEventType.SETTLEMENT;
        this.tradeId = settlementRef;
        this.accountId = accountId;
        this.price = amount;
    }

    /** 저널 핸들러가 기록을 마친 뒤 그 시점의 저널 위치를 슬롯에 남긴다(1-2). 비즈니스 핸들러가 읽는다. */
    public void setJournaledPosition(long journaledPosition) {
        this.journaledPosition = journaledPosition;
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
        this.sourcePosition = 0L;
        this.sourceSessionId = 0;
        this.journaledPosition = 0L;
        this.publishedAtEpochNanos = 0L;
    }

    public AccountEventType getType() {
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

    public long getSourcePosition() {
        return sourcePosition;
    }

    public int getSourceSessionId() {
        return sourceSessionId;
    }

    public long getJournaledPosition() {
        return journaledPosition;
    }

    public long getPublishedAtEpochNanos() {
        return publishedAtEpochNanos;
    }
}
