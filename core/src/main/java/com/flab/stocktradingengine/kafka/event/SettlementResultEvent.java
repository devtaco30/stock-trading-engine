package com.flab.stocktradingengine.kafka.event;

import java.math.BigDecimal;

/**
 * settlement(T+2)가 account-settlements 토픽으로 발행하는, "이 계좌 실제 잔고를 amount 만큼
 * 차감하라"는 명령 이벤트. settlementRef는 멱등키(같은 정산이 다시 와도 한 번만 반영).
 *
 * <p>v2 전용 신규 경로 — v1엔 이 되돌림 자체가 없었다(수동 repay API가 DB를 직접 차감했다).</p>
 */
public record SettlementResultEvent(
    Long settlementRef,
    Long accountId,
    BigDecimal amount
) {}
