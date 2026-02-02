package com.flab.stocktradingengine.account.entity;

public enum AccountStatus {
    ACTIVE, // 정상
    IN_ARREARS, // 미수금 발생 계좌
    FROZEN // 동결 게좌
}
