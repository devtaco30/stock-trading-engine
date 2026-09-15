package com.flab.stocktradingengine.account.projection.worker.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 계좌 보유 read model 한 행(v2 전용, 계좌 상태 영속/프로젝션 트랙 U3). {@code AccountStateEvent}가
 * 델타가 아니라 매번 그 계좌의 보유 전체를 담으므로, 반영 시 이 계좌의 기존 행 전부를 지우고
 * 이벤트의 map으로 다시 채운다(전체 교체) — {@code AccountProjectionUpserter} 참고.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Getter
@Entity
@IdClass(AccountProjectionHoldingId.class)
@Table(name = "account_projection_holding")
public class AccountProjectionHolding {

    @Id
    @Column(name = "account_id")
    private Long accountId;

    @Id
    @Column(name = "stock_code", length = 10)
    private String stockCode;

    @Column(nullable = false)
    private int quantity;

    public AccountProjectionHolding(Long accountId, String stockCode, int quantity) {
        this.accountId = accountId;
        this.stockCode = stockCode;
        this.quantity = quantity;
    }
}
