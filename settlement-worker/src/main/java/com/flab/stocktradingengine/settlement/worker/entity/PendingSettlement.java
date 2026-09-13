package com.flab.stocktradingengine.settlement.worker.entity;

import java.math.BigDecimal;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 정산 대기 중인 미수금 한 건.
 *
 * <p>account-worker가 settlement-requests로 보낸 통지를 그대로 저장한다(v1 {@code Unpaid}와
 * 달리 Account·Order 연관관계 없는 독립 엔티티 — 이 워커는 계좌·주문 DB를 갖지 않는다).
 * settlementRef(=tradeId)를 자연 PK로 써서 존재 여부만으로 중복 저장을 막는다.</p>
 *
 * <p>{@code (status, due_at_epoch_millis)} 복합 인덱스는 T+2 스캔 쿼리
 * ({@code findByStatusAndDueAtEpochMillisLessThanEqual}, a2-3)가 status로 좁힌 뒤
 * dueAtEpochMillis로 범위 조회하기 때문이다.</p>
 */
@NoArgsConstructor
@Getter
@Entity
@Table(
    name = "pending_settlements",
    indexes = @Index(name = "idx_pending_settlements_status_due_at", columnList = "status, due_at_epoch_millis")
)
public class PendingSettlement {

    @Id
    @Column(name = "settlement_ref")
    private Long settlementRef;

    @Column(nullable = false)
    private Long accountId;

    @Column(nullable = false, precision = 15, scale = 0)
    private BigDecimal amount;

    @Column(nullable = false)
    private long dueAtEpochMillis;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private SettlementStatus status;

    public PendingSettlement(Long settlementRef, Long accountId, BigDecimal amount, long dueAtEpochMillis) {
        this.settlementRef = settlementRef;
        this.accountId = accountId;
        this.amount = amount;
        this.dueAtEpochMillis = dueAtEpochMillis;
        this.status = SettlementStatus.PENDING;
    }

    /**
     * account-settlements 발행이 끝난 뒤 SETTLED로 전이한다.
     *
     * @throws IllegalStateException 이미 SETTLED면(정상 흐름이면 일어날 수 없는 불변식 위반 —
     *                                fail-fast로 잡는다)
     */
    public void markSettled() {
        if (status == SettlementStatus.SETTLED) {
            throw new IllegalStateException("이미 정산 완료된 건입니다: settlementRef=" + settlementRef);
        }
        this.status = SettlementStatus.SETTLED;
    }
}
