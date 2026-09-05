package com.flab.stocktradingengine.account.entity;

import java.math.BigDecimal;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
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
 * 보유 주식
 * <p>계좌별 종목 보유 수량·평균 매수가. 매수 체결 시 추가/합산, 매도 체결 시 차감.</p>
 */
@NoArgsConstructor
@Getter
@Entity
@Table(name = "holdings")
public class Holding {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "account_id", nullable = false)
    private Account account;

    @Column(nullable = false, length = 10)
    private String stockCode;

    @Column(nullable = false)
    private int quantity;

    @Column(nullable = false, precision = 12, scale = 0)
    private BigDecimal averagePrice;

    public Holding(Account account, String stockCode, int quantity, BigDecimal averagePrice) {
        this.account = account;
        this.stockCode = stockCode;
        this.quantity = quantity;
        this.averagePrice = averagePrice;
    }

    /**
     * 체결(매수)을 적용한다. 수량을 더하고 체결 단가를 반영해 평균 단가를 갱신한다.
     */
    public void applyExecution(int quantity, BigDecimal executionPrice) {
        int totalQty = this.quantity + quantity;
        BigDecimal existingAmount = this.averagePrice.multiply(BigDecimal.valueOf(this.quantity));
        BigDecimal additionalAmount = executionPrice.multiply(BigDecimal.valueOf(quantity));
        BigDecimal totalAmount = existingAmount.add(additionalAmount);
        this.quantity = totalQty;
        this.averagePrice = totalAmount.divide(BigDecimal.valueOf(totalQty), 0, java.math.RoundingMode.DOWN);
    }

    /**
     * 보유 수량 차감(매도 체결 반영). 보유보다 많이 차감하려 하면 예외로 막는다.
     * 주문 시점의 가용 수량 검증(OrderWriter)이 정상이면 이 가드는 발동하지 않는다.
     * 그 방어가 뚫렸을 때 음수 보유가 저장되는 것을 마지막에 차단하는 안전망이다.
     */
    public void subtractQuantity(int subQuantity) {
        if (subQuantity > this.quantity) {
            throw new IllegalStateException(
                "보유 수량 초과 차감: 보유=" + this.quantity + ", 차감=" + subQuantity);
        }
        this.quantity -= subQuantity;
    }
}
