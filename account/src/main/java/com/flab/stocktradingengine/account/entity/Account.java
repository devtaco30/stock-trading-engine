package com.flab.stocktradingengine.account.entity;

import java.math.BigDecimal;

import com.flab.stocktradingengine.support.SnowflakeId;
import com.flab.stocktradingengine.user.entity.User;

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
    private Long id;

    @SnowflakeId
    @Column(name = "account_id", nullable = false, updatable = false)
    private Long accountId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", nullable = false)
    private User user;

    @Column(nullable = false, precision = 19, scale = 0)
    private BigDecimal balance; // 총 현금 잔액

    @Column(nullable = false, precision = 5, scale = 2)
    private BigDecimal marginRate; // 증거금률 비율 (0.40, 1.00)

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private AccountStatus status;

    /** 더미/초기화용: user, balance, marginRate, status로 생성. accountId는 INSERT 시 Snowflake 생성. */
    public Account(User user, BigDecimal balance, BigDecimal marginRate, AccountStatus status) {
        this.user = user;
        this.balance = balance;
        this.marginRate = marginRate;
        this.status = status;
    }

    public void changeStatus(AccountStatus status) {
        this.status = status;
    }

    public void changeBalance(BigDecimal balance) {
        this.balance = balance;
    }

    public void changeMarginRate(BigDecimal marginRate) {
        this.marginRate = marginRate;
    }

    /**
     * 이 계좌의 소유자가 주어진 사용자인지 여부.
     */
    public boolean isOwnedBy(Long userId) {
        return user != null && userId != null && userId.equals(user.getId());
    }
}
