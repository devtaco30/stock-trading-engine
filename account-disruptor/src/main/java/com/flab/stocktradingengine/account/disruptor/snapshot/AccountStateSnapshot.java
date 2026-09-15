package com.flab.stocktradingengine.account.disruptor.snapshot;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.flab.stocktradingengine.account.disruptor.domain.AccountState;

/**
 * 계좌 하나의 상태 스냅샷(2d-2). {@link AccountState#toSnapshot()}이 찍고
 * {@link AccountState#AccountState(AccountStateSnapshot)}가 그대로 되살린다.
 *
 * <p>{@code tradeIdGenerations}·{@code processedSettlementRefs}(체결·정산 멱등 캐시)를 반드시
 * 같이 담는다 — 빼면 복구 뒤 Kafka가 재전송한 체결·정산이 "새 이벤트"로 오인돼 중복 반영된다
 * (matching {@code filledOrderTimestamps}와 같은 이유). {@code tradeIdGenerations}는 현재 세대가
 * 0번째인 순서로 담는다(1-4) — 세대별 상한(2×N건치)으로 무한 증가를 막는다.</p>
 *
 * <p>{@code seq}(계좌별 단조 카운터, 계좌 상태 영속/프로젝션 트랙 Unit 1)도 반드시 같이 담는다 —
 * 빼면 복원 직후 seq가 0으로 되돌아가, 그 뒤 저널 replay로 재현한 값이 크래시 전보다 작아져서
 * 프로젝션 워커의 stale-guard(더 큰 seq만 반영)가 최신 상태를 구버전으로 오인해 버릴 수 있다.</p>
 */
public record AccountStateSnapshot(
    long accountId,
    long seq,
    BigDecimal balance,
    BigDecimal marginRate,
    Map<Long, BuyReservationSnapshot> reservations,
    Map<Long, SellReservationSnapshot> sellReservations,
    Map<String, Integer> holdings,
    List<TradeIdGenerationSnapshot> tradeIdGenerations,
    Set<Long> processedSettlementRefs,
    Set<String> processedRequestIds,
    BigDecimal unpaid
) {
}
