package com.flab.stocktradingengine.matching.worker.metrics;

/** 매칭 엔진 한 시점의 계측 값. 남은 칸과 queue 깊이는 그 순간 값이고 나머지(전량 체결 주문 수 포함)는 시작부터의 누적이다. */
record MatchingEngineMetrics(
        long ringRemaining, long ringFullWaits, long journalBackPressure, long journalOtherRetries,
        long fillQueueDepth, long fillEnqueueWaits, long fullyFilledOrders) {
}
