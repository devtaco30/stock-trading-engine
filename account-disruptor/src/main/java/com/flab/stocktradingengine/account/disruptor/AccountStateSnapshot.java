package com.flab.stocktradingengine.account.disruptor;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;

/**
 * 계좌 하나의 상태 스냅샷(2d-2). {@link AccountState#toSnapshot()}이 찍고
 * {@link AccountState#AccountState(AccountStateSnapshot)}가 그대로 되살린다.
 *
 * <p>{@code processedTradeIds}·{@code processedSettlementRefs}(체결·정산 멱등 캐시)를 반드시
 * 같이 담는다 — 빼면 복구 뒤 Kafka가 재전송한 체결·정산이 "새 이벤트"로 오인돼 중복 반영된다
 * (matching {@code filledOrderTimestamps}와 같은 이유).</p>
 */
public record AccountStateSnapshot(
    long accountId,
    BigDecimal balance,
    BigDecimal marginRate,
    Map<Long, BuyReservationSnapshot> reservations,
    Map<Long, SellReservationSnapshot> sellReservations,
    Map<String, Integer> holdings,
    Set<Long> processedTradeIds,
    Set<Long> processedSettlementRefs,
    Map<String, Long> requestIdToOrderId,
    BigDecimal unpaid
) {
}
