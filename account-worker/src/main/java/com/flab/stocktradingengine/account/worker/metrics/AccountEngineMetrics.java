package com.flab.stocktradingengine.account.worker.metrics;

/**
 * 계좌 엔진 안에서 센 값의 한 시점 모습(12-3절 2번 표의 3단계와 4단계).
 *
 * @param ringRemaining 계좌 ring buffer의 남은 빈 칸 수(읽는 순간의 값, 엔진이 시작되기 전이면 -1)
 * @param ringFullWaits 빈 칸이 없어 발행이 기다린 횟수(누적, 발행 호출 단위)
 * @param journalBackPressure 계좌 저널 offer가 BACK_PRESSURED로 되돌아와 다시 시도한 횟수(누적, offer 호출 단위)
 * @param journalOtherRetries 계좌 저널 offer가 ADMIN_ACTION, NOT_CONNECTED 등 그 밖의 음수로 되돌아와 다시 시도한 횟수(누적, offer 호출 단위)
 */
public record AccountEngineMetrics(long ringRemaining, long ringFullWaits, long journalBackPressure, long journalOtherRetries) {
}
