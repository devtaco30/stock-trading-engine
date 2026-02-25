package com.flab.stocktradingengine.account.entity;

import java.time.Instant;

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

import com.flab.stocktradingengine.user.entity.User;

import org.hibernate.annotations.Generated;
import org.hibernate.generator.EventType;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 계좌 상태 변경 이력
 * <p>누가(시스템/관리자), 언제, 무엇에서 무엇으로, 사유를 기록한다. changedByUser가 null이면 시스템에 의한 변경.</p>
 */
@Entity
@Table(name = "account_status_histories")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Getter
public class AccountStatusHistory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "account_id", nullable = false)
    private Account account;

    @Enumerated(EnumType.STRING)
    @Column(name = "from_status", nullable = false, length = 20)
    private AccountStatus fromStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "to_status", nullable = false, length = 20)
    private AccountStatus toStatus;

    @Generated(event = EventType.INSERT)
    @Column(nullable = false, insertable = false, columnDefinition = "TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP")
    private Instant changedAt;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "changed_by_user_id")
    private User changedByUser;

    @Column(length = 500)
    private String reason;

    public AccountStatusHistory(
        Account account,
        AccountStatus fromStatus,
        AccountStatus toStatus,
        User changedByUser,
        String reason
    ) {
        this.account = account;
        this.fromStatus = fromStatus;
        this.toStatus = toStatus;
        this.changedByUser = changedByUser;
        this.reason = reason;
    }
}
