package com.flab.stocktradingengine.time;

/**
 * 구간 분해 측정의 단계 하나에 대한 스냅샷. 값은 마이크로초 단위다 — 시각의 해상도가 1µs라서
 * (decision: lld-C-segments.md 8-3절, 측정 U1) 그보다 잘게 적어도 정보가 없다. 평균은 단계 값을 더해
 * 전체와 맞춰 보는 합 정합성 검사에 쓴다(백분위는 더할 수 없지만 평균은 더할 수 있다).
 */
public record StageSnapshot(long count, long p50Micros, long p95Micros, long p99Micros, long maxMicros,
                            double meanMicros) {

    private static final StageSnapshot EMPTY = new StageSnapshot(0L, 0L, 0L, 0L, 0L, 0.0);

    /** 표본이 하나도 없는 단계의 스냅샷. */
    public static StageSnapshot empty() {
        return EMPTY;
    }
}
