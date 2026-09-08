package com.flab.stocktradingengine.settlement.worker;

import java.math.BigDecimal;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 정산 대기 중인 미수금 한 건.
 *
 * <p>account-worker가 settlement-requests로 보낸 통지를 그대로 저장한다(v1 {@code Unpaid}와
 * 달리 Account·Order 연관관계 없는 독립 엔티티 — 이 워커는 계좌·주문 DB를 갖지 않는다).
 * settlementRef(=tradeId)를 자연 PK로 써서 존재 여부만으로 중복 저장을 막는다.</p>
 */
@NoArgsConstructor
@Getter
@Entity
@Table(name = "pending_settlements")
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
}
