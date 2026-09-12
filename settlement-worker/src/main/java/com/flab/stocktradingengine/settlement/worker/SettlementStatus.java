package com.flab.stocktradingengine.settlement.worker;

/**
 * 미수금 정산 상태
 */
public enum SettlementStatus {
    PENDING, // 정산 대기 (T+2 도래 전)
    SETTLED  // 정산 완료
}
