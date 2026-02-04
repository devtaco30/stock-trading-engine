package com.flab.stocktradingengine.trading.entity;

import java.math.BigDecimal;
import java.time.Instant;

import com.flab.stocktradingengine.account.entity.Account;

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
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 주문
 * <p>매수/매도 주문. PENDING 시 증거금 홀딩(heldMargin), 체결 시 FILLED.</p>
 */
@NoArgsConstructor
@Getter
@Entity
@Table(name = "orders")
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String orderId;

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

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderStatus status = OrderStatus.PENDING;

    @Column(nullable = false)
    private Instant orderAt;

    @Column
    private Instant filledAt;

    /** 매수 주문 시 홀딩된 증거금. 매도/취소/체결 시 null 또는 0 */
    @Column(precision = 15, scale = 0)
    private BigDecimal heldMargin; // 매수 주문 시 홀딩된 증거금

    public void fill(Instant filledAt) {
        this.status = OrderStatus.FILLED;
        this.filledAt = filledAt;
    }

    public void cancel() {
        this.status = OrderStatus.CANCELLED;
        this.heldMargin = null;
    }
}
