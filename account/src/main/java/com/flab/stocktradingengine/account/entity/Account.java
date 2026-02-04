package com.flab.stocktradingengine.account.entity;

import java.math.BigDecimal;

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
 * 계좌
 * <p>총 현금(balance), 증거금률(marginRate), 상태(status)를 저장.</p>
 * <p>출금 가능 잔액·총 자산·미결제·매도 예정·매수 가능 금액은 보유·주문·정산 데이터로 계산.</p>
 */
@NoArgsConstructor
@Getter
@Entity
@Table(name = "accounts")
public class Account {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id; // DB 에서의 관리를 위한 sequence 값

    @Column(nullable = false, unique = true)
    private String accountId; // 계좌 고유 id -> 순차적 증가보다는 256 hash 또는 uuid 등으로 생성(?)

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", nullable = false)
    private User user;

    @Column(nullable = false, precision = 19, scale = 0)
    private BigDecimal balance; // 총 현금 잔액

    @Column(nullable = false)
    private int marginRate; // 증거금률 (40, 100 %)

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private AccountStatus status;

    public void changeStatus(AccountStatus status) {
        this.status = status;
    }

    public void changeBalance(BigDecimal balance) {
        this.balance = balance;
    }

    public void changeMarginRate(int marginRate) {
        this.marginRate = marginRate;
    }
}
