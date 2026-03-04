package com.flab.stocktradingengine.trading.entity;

import java.math.BigDecimal;
import java.time.Instant;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.support.SnowflakeId;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
/**
 * 주문
 * <p>매수/매도 주문. PENDING 시 증거금 예약(reservedMargin), 체결 시 FILLED.</p>
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Entity
@Table(name = "orders")
@Builder
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @SnowflakeId
    @Column(name = "order_id", nullable = false, unique = true, updatable = false)
    private Long orderId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "account_id", nullable = false)
    private Account account;

    @Column(nullable = false, length = 10)
    private String stockCode; // 주식 종목 코드

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderSide side; // 주문 방향 (매수/매도)

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderType orderType; // 주문 유형 (지정가/시장가)

    @Column(nullable = false, precision = 12, scale = 0)
    private BigDecimal price; // 주문 가격

    @Column(nullable = false)
    private int quantity;

    /**
     * 누적 체결 수량. 부분 체결이 발생할 때마다 증가하며, quantity 와 같아지면 FILLED.
     * DB 에 저장되어 서버 재시작 시 호가창 복원(OrderBookInitializer)에 활용된다.
     */
    @Builder.Default
    @Column(nullable = false, columnDefinition = "integer not null default 0")
    private int filledQuantity = 0;

    @Builder.Default
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderStatus status = OrderStatus.PENDING;

    @Column(nullable = false)
    private Instant orderAt;

    @Column
    private Instant filledAt;

    /** 매수 주문 시 예약된 증거금. 매도/취소/체결 시 null 또는 0 */
    @Column(precision = 15, scale = 0)
    private BigDecimal reservedMargin;

    /** 전량 체결. 기존 수동 체결 흐름(fillOrder REST)과의 호환용으로 유지. */
    public void fill(Instant filledAt) {
        this.filledQuantity = this.quantity;
        this.status = OrderStatus.FILLED;
        this.filledAt = filledAt;
    }

    /**
     * 부분/전량 체결 반영.
     * <p>매칭 엔진이 인메모리에서 수량을 결정한 뒤, DB 반영 시 이 메서드를 호출한다.
     * filledQuantity 가 quantity 에 도달하면 자동으로 FILLED 처리.</p>
     *
     * @throws IllegalArgumentException fillQty 가 0 이하이거나 남은 수량을 초과할 때
     */
    public void addFilled(int fillQty, Instant now) {
        if (fillQty <= 0 || this.filledQuantity + fillQty > this.quantity) {
            throw new IllegalArgumentException(
                "잘못된 체결 수량: fillQty=" + fillQty
                + ", filled=" + filledQuantity + ", quantity=" + quantity);
        }
        this.filledQuantity += fillQty;
        if (this.filledQuantity >= this.quantity) {
            this.status = OrderStatus.FILLED;
            this.filledAt = now;
        }
    }

    /** 미체결 잔량. 매칭 루프 종료 조건에 사용. */
    public int getRemainingQuantity() {
        return Math.max(0, quantity - filledQuantity);
    }

    public void cancel() {
        this.status = OrderStatus.CANCELLED;
        this.reservedMargin = null;
    }
}
