package com.flab.stocktradingengine.trading.service;

/**
 * 구간 분해 측정(measure/segments 브랜치 한정)에서 v1 주문 접수 경로(order-engine 컨슈머 → 서비스 → 저장)의
 * 지점 시각을 한 주문 단위로 모아 컨슈머에게 돌려주는 객체. 컨슈머 스레드 하나 안에서만 쓰이므로
 * 동기화하지 않는다. 계측이 꺼져 있으면 만들지 않고 시각 인자가 없는 기존 메서드를 부른다.
 *
 * <ul>
 *   <li>{@code receivedAtNanos} — V-P1, 컨슈머 리스너 진입</li>
 *   <li>{@code idemCheckedAtNanos} — V-P3, 멱등 조회 반환 직후</li>
 *   <li>{@code lockedAtNanos} — V-P4, 계좌(또는 보유) 행 락 획득 직후</li>
 *   <li>{@code summedAtNanos} — V-P5, 합계 쿼리 직후</li>
 *   <li>{@code decidedAtNanos} — V-P5b, 한도 검사 통과 직후(판정 도달)</li>
 *   <li>{@code insertedAtNanos} — V-P6, {@code saveAndFlush} 직후</li>
 * </ul>
 * 거부되거나 이미 접수된 요청은 해당 지점 이후의 값이 0으로 남는다.
 */
public final class OrderStageTimes {

    private final long receivedAtNanos;
    private long idemCheckedAtNanos;
    private long lockedAtNanos;
    private long summedAtNanos;
    private long decidedAtNanos;
    private long insertedAtNanos;

    private OrderStageTimes(long receivedAtNanos) {
        this.receivedAtNanos = receivedAtNanos;
    }

    public static OrderStageTimes startingAt(long receivedAtNanos) {
        return new OrderStageTimes(receivedAtNanos);
    }

    public void markIdemChecked(long nanos) {
        this.idemCheckedAtNanos = nanos;
    }

    public void markLocked(long nanos) {
        this.lockedAtNanos = nanos;
    }

    public void markSummed(long nanos) {
        this.summedAtNanos = nanos;
    }

    public void markDecided(long nanos) {
        this.decidedAtNanos = nanos;
    }

    public void markInserted(long nanos) {
        this.insertedAtNanos = nanos;
    }

    public long receivedAtNanos() {
        return receivedAtNanos;
    }

    public long idemCheckedAtNanos() {
        return idemCheckedAtNanos;
    }

    public long lockedAtNanos() {
        return lockedAtNanos;
    }

    public long summedAtNanos() {
        return summedAtNanos;
    }

    public long decidedAtNanos() {
        return decidedAtNanos;
    }

    public long insertedAtNanos() {
        return insertedAtNanos;
    }
}
