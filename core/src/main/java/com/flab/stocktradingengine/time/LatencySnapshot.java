package com.flab.stocktradingengine.time;

/**
 * {@link LatencyHistogram#snapshot()}이 만드는 접수 지연 백분위 스냅샷(나노초 단위) —
 * decision_records/v1-v2-e2e-measurement.md 끝점①(주문 접수·예약)의 측정 결과.
 */
public record LatencySnapshot(long count, long p50Nanos, long p95Nanos, long p99Nanos, long maxNanos) {

    private static final LatencySnapshot EMPTY = new LatencySnapshot(0L, 0L, 0L, 0L, 0L);

    /** 측정이 꺼져 있거나 표본이 하나도 없을 때 돌려주는 빈 스냅샷. */
    public static LatencySnapshot empty() {
        return EMPTY;
    }
}
