package com.flab.stocktradingengine.api.projection.entity;

import java.io.Serializable;
import java.util.Objects;

/**
 * {@link AccountProjectionHolding}의 복합키(accountId, stockCode). account-projection-worker의
 * 같은 이름 클래스와 별개 매핑이다(계좌 상태 프로젝션 트랙 U4, CQRS Option 1 — 쓰기·읽기를
 * DB 스키마로만 잇는다).
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
