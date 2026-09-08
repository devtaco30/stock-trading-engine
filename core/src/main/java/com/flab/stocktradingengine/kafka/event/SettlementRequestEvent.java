package com.flab.stocktradingengine.kafka.event;

import java.math.BigDecimal;

/**
 * account-worker가 settlement-requests 토픽으로 발행하는, "매수 체결로 이 계좌에 미수금이
 * 생겼다"는 통지 이벤트. amount는 account-worker(AccountState.applyBuyFill)가 계산한 값
 * 그대로다 — settlement은 재계산하지 않는다(부분체결 반올림 텔레스코핑이 밖에서 재계산하면 어긋난다).
 *
 * <p>settlementRef는 tradeId를 그대로 쓴다(체결 하나당 정산 하나, 멱등키).
 * dueAtEpochMillis는 정산 가능 시각(절대시각, epoch millis)이다 — 계산 시점의 호스트 시각 기준
 * T+2. {@code LocalDate}가 아니라 절대시각을 쓰는 이유는 타임존에 따라 "며칠"의 경계가 갈려
 * 비교 시 드리프트가 날 수 있어서다. 코어는 시간을 모르므로 호스트(account-worker)가 계산한다.</p>
 */
public record SettlementRequestEvent(
    Long settlementRef,
    Long accountId,
    BigDecimal amount,
    long dueAtEpochMillis
) {}
