package com.flab.stocktradingengine.account.projection.worker.entity;

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
 * 계좌 잔고 read model 한 행(v2 전용, 계좌 상태 영속/프로젝션 트랙 U3). account-worker가 발행한
 * {@code AccountStateEvent}(full-state)를 그대로 upsert한다 — 델타가 아니라 매번 전체를 덮어쓴다.
 *
 * <p>v1 {@code Account}와 무관한 독립 테이블이다(v1은 이 테이블을 안 쓰고, 이 워커도 v1 테이블에
 * 손대지 않는다) — write 충돌 회피가 v2 전용 테이블을 신설한 이유다.</p>
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

    /**
     * stale-guard: 들어온 이벤트의 seq가 이미 저장된 seq보다 클 때만 반영한다(계좌 상태 영속/
     * 프로젝션 트랙 U3). {@code <=}면 순서 역전이거나 재도착(멱등)이라 무시한다 — at-least-once +
     * full-state 재발행 구조라 무해하다(다음 최신 이벤트가 결국 덮는다).
     *
     * @return 이번 호출로 실제 반영했으면 true, seq가 stale이라 무시했으면 false
     */
    public boolean applyIfNewer(BigDecimal newBalance, long newSeq, Instant newUpdatedAt) {
        if (newSeq <= this.seq) {
            return false;
        }
        this.balance = newBalance;
        this.seq = newSeq;
        this.updatedAt = newUpdatedAt;
        return true;
    }
}
