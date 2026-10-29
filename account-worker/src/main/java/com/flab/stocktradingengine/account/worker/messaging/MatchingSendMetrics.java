package com.flab.stocktradingengine.account.worker.messaging;

/**
 * 계좌 shard의 매칭행 발신에서 센 값의 한 시점 모습(설계서 3-6절).
 *
 * @param outboxSize 매칭행 queue({@code outbox})에 지금 쌓여 있는 주문 수
 * @param enqueueWaitCount 계좌 엔진 스레드가 queue가 가득 차 넣지 못하고 기다린 횟수(누적)
 * @param enqueueWaitNanos 그 기다린 시간의 합(나노초, 누적)
 * @param offerRetryCount sender thread의 Aeron offer가 음수를 돌려줘 다시 시도한 횟수(누적)
 */
public record MatchingSendMetrics(int outboxSize, long enqueueWaitCount, long enqueueWaitNanos, long offerRetryCount) {
}
