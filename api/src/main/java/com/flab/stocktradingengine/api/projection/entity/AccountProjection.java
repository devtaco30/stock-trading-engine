package com.flab.stocktradingengine.api.projection.entity;

import java.math.BigDecimal;
import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 계좌 잔고 read model 조회 전용 매핑(계좌 상태 프로젝션 트랙 U4). account-projection-worker의
 * 같은 이름 엔티티와 별개 클래스다 — 같은 테이블(account_projection)을 CQRS 정석대로 쓰기(worker)·
 * 읽기(api)가 각자 독립적으로 매핑한다(Option 1, ADR 감). 이 쪽은 조회만 하므로 setter·상태
 * 변경 메서드가 없다.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Getter
@Entity
@Table(name = "account_projection")
public class AccountProjection {

    @Id
    @Column(name = "account_id")
    private Long accountId;

    @Column(nullable = false, precision = 19, scale = 0)
    private BigDecimal balance;

    @Column(nullable = false)
    private long seq;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public AccountProjection(Long accountId, BigDecimal balance, long seq, Instant updatedAt) {
        this.accountId = accountId;
        this.balance = balance;
        this.seq = seq;
        this.updatedAt = updatedAt;
    }
}
