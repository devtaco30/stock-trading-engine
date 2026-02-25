package com.flab.stocktradingengine.settlement.entity;

/**
 * 미결제 상태
 */
public enum UnpaidStatus {
    PENDING,  // 결제 대기 (T+2 납부 전)
    SETTLED   // 결제 완료
}
