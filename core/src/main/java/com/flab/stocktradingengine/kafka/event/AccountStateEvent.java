package com.flab.stocktradingengine.kafka.event;

import java.math.BigDecimal;
import java.util.Map;

/**
 * account-worker가 계좌 상태 프로젝션 트랙(off-path)에서 {@code account-state} 토픽으로 발행하는
 * full-state 이벤트. 델타가 아니라 그 시점 잔고·보유 전체를 담는다 — 프로젝션 워커가 upsert로
 * 그대로 반영한다(계좌 상태 영속/프로젝션 트랙 Unit 3).
 *
 * <p>seq는 계좌별 단조 카운터(account-disruptor {@code AccountState}, Unit 1) — 프로젝션
 * 워커가 stale-guard(더 큰 seq만 반영)로 순서 역전·중복을 무해하게 흡수한다. epochMillis는
 * 캡처 시각(절대시각) — {@code LocalDate}가 아니라 절대시각을 쓰는 이유는 타임존에 따라 날짜
 * 경계가 갈려 비교 시 드리프트가 날 수 있어서다({@link SettlementRequestEvent}와 같은 이유).</p>
 */
public record AccountStateEvent(
    long accountId,
    BigDecimal balance,
    Map<String, Integer> holdings,
    long seq,
    long epochMillis
) {}
