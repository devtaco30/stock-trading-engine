package com.flab.stocktradingengine.account.disruptor.snapshot;

import java.util.Set;

/**
 * tradeId 멱등 장부 세대 한 줄의 스냅샷(1-4, {@code docs/_tradeid_snapshot_prune.html}).
 * {@code AccountState.TradeIdGeneration}은 private record라 엔진 밖으로 노출되지 않으므로,
 * 스냅샷이 볼 수 있는 형태로 따로 둔다({@link BuyReservationSnapshot}과 같은 이유).
 *
 * <p>{@code boundarySeq}는 이 세대가 시작된 시점의 저널 적용 순번이다 — 복구 후에도 세대 경계를
 * 그대로 유지해야 이후 라이브에서 durable 경계를 판단할 수 있다.</p>
 */
public record TradeIdGenerationSnapshot(long boundarySeq, Set<Long> tradeIds) {
}
