package com.flab.stocktradingengine.account.entity;

public enum AccountStatus {
    ACTIVE,   // 정상
    IN_ARREARS, // 미수금 발생 계좌
    FROZEN;  // 동결 계좌

    /**
     * 이 상태에서 주어진 상태로의 전이가 허용되는지 여부.
     */
    public boolean canTransitionTo(AccountStatus to) {
        if (this == to) {
            return false;
        }
        return switch (this) {
            case ACTIVE -> to == IN_ARREARS || to == FROZEN;
            case IN_ARREARS -> to == ACTIVE || to == FROZEN;
            case FROZEN -> to == ACTIVE;
        };
    }
}
