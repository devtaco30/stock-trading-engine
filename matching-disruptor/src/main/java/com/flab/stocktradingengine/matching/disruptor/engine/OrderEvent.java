package com.flab.stocktradingengine.matching.disruptor.engine;

import java.math.BigDecimal;
import java.time.Instant;

import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.codec.EventType;

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
    // JournalEventHandler(저널 게이팅 단계)가 기록 직후 채운다. 매칭 핸들러(마지막 소비자)는 이
    // 슬롯을 그대로 읽어 "이 이벤트까지 저널에 반영됐다"는 위치를 얻는다 — account-disruptor
    // AccountEvent.journaledPosition과 같은 이유(I6 U1).
    private long journaledPosition;
    // 이 주문을 실어 보낸 인테이크 수신 스트림의 위치·발행자(I2 U1) — account-disruptor
    // AccountEvent.sourcePosition/sourceSessionId와 같은 이유. 소비자(매칭 핸들러)가 이 주문을
    // 실제로 반영한 뒤에야 "적용 완료 위치"로 인정한다.
    private long sourcePosition;
    private int sourceSessionId;

    /**
     * 주문 접수 명령으로 슬롯을 채운다. sourcePosition·sourceSessionId 를 모르는 발행자(저널 replay,
     * 테스트 등)를 위한 오버로드 — 둘 다 0으로 채운다(발행자가 하나뿐이면 항상 0이라 구분이
     * 필요 없다, account-disruptor {@code AccountEvent}와 같은 관례).
     */
    public void setPlace(long orderId, long accountId, String stockCode,
                         OrderSide side, BigDecimal price, int quantity, Instant orderAt) {
        setPlace(orderId, accountId, stockCode, side, price, quantity, orderAt, 0L, 0);
    }

    /**
     * 주문 접수 명령으로 슬롯을 채운다. sourcePosition 은 이 주문을 실어 보낸 인테이크 수신
     * 스트림의 위치(I2 U1) — 소비자가 실제로 이 주문을 반영한 뒤에야 "적용 완료 위치"로 인정한다.
     * sourceSessionId 는 그 위치가 어느 발행자(Aeron 연결=계좌 샤드)의 것인지 구분하는 키 — 계좌가
     * 여럿이면 recording 이 여럿 생기고, position 값 하나만으로는 어느 recording의 위치인지
     * 구분할 수 없다(account-disruptor {@code AccountEvent.setBuyFill}과 같은 이유).
     */
    public void setPlace(long orderId, long accountId, String stockCode,
                         OrderSide side, BigDecimal price, int quantity, Instant orderAt,
                         long sourcePosition, int sourceSessionId) {
        this.type = EventType.PLACE;
        this.orderId = orderId;
        this.accountId = accountId;
        this.stockCode = stockCode;
        this.side = side;
        this.price = price;
        this.quantity = quantity;
        this.orderAt = orderAt;
        this.sourcePosition = sourcePosition;
        this.sourceSessionId = sourceSessionId;
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
        this.journaledPosition = 0L;
        this.sourcePosition = 0L;
        this.sourceSessionId = 0;
    }

    public void setJournaledPosition(long journaledPosition) {
        this.journaledPosition = journaledPosition;
    }

    public long getJournaledPosition() {
        return journaledPosition;
    }

    public long getSourcePosition() {
        return sourcePosition;
    }

    public int getSourceSessionId() {
        return sourceSessionId;
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
