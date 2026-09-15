package com.flab.stocktradingengine.account.projection.worker.entity;

import java.io.Serializable;
import java.util.Objects;

/**
 * {@link AccountProjectionHolding}의 복합키(accountId, stockCode) — 이 레포 최초의 JPA
 * {@code @IdClass}(계좌 상태 영속/프로젝션 트랙 U3). 보유는 계좌당 종목별로 유일하며, 서로게이트
 * id를 둘 이유가 없어(별도 참조·정렬 요구가 없다) DDL이 지정한 자연 복합키를 그대로 쓴다.
 */
public class AccountProjectionHoldingId implements Serializable {

    private Long accountId;
    private String stockCode;

    public AccountProjectionHoldingId() {
    }

    public AccountProjectionHoldingId(Long accountId, String stockCode) {
        this.accountId = accountId;
        this.stockCode = stockCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AccountProjectionHoldingId that)) {
            return false;
        }
        return Objects.equals(accountId, that.accountId) && Objects.equals(stockCode, that.stockCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accountId, stockCode);
    }
}
