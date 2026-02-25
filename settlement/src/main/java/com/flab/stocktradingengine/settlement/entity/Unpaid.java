package com.flab.stocktradingengine.settlement.entity;

import java.math.BigDecimal;
import java.time.LocalDate;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.trading.entity.Order;

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
 * 미결제
 * <p>매수 체결 후 T+2 결제일까지 미납 금액. 체결 시 생성, 결제일 정산 시 SETTLED.</p>
 */
@NoArgsConstructor
@Getter
@Entity
@Table(name = "unpaids")
public class Unpaid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String unpaidId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "account_id", nullable = false)
    private Account account;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "order_id", nullable = false)
    private Order order;

    @Column(nullable = false, precision = 15, scale = 0)
    private BigDecimal amount;

    @Column(nullable = false)
    private LocalDate settlementDate;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private UnpaidStatus status = UnpaidStatus.PENDING;

    public Unpaid(String unpaidId, Account account, Order order, BigDecimal amount, LocalDate settlementDate) {
        this.unpaidId = unpaidId;
        this.account = account;
        this.order = order;
        this.amount = amount;
        this.settlementDate = settlementDate;
    }

    /** 정산 완료 처리 (T+2 변제 등). */
    public void markSettled() {
        this.status = UnpaidStatus.SETTLED;
    }
}
