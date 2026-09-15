package com.flab.stocktradingengine.api.projection.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 계좌 보유 read model 조회 전용 매핑(계좌 상태 프로젝션 트랙 U4). account-projection-worker의
 * 같은 이름 엔티티와 별개 클래스다 — 같은 테이블(account_projection_holding)을 CQRS 정석대로
 * 쓰기(worker)·읽기(api)가 각자 독립적으로 매핑한다. 조회만 하므로 setter가 없다.
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
