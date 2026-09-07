package com.flab.stocktradingengine.account.disruptor;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * 계좌 하나의 인메모리 상태.
 *
 * <p>소비자 스레드 하나만 이 객체를 만진다(single writer) — 그래서 락이 없다.
 * 매수 검증·예약을 이 객체가 직접 책임한다(Rich Domain). 산식은 v1 {@code OrderWriter.writeBuyOrder}
 * 의 DB 락 안 계산을, DB·SUM 쿼리 없이 인메모리 러닝 합으로 옮긴 것이다.</p>
 */
public final class AccountState {

    private final long accountId;
    private final BigDecimal balance;      // 총 현금 잔액
    private final BigDecimal marginRate;   // 증거금률 (0.40 ~ 1.00, 시드값 전제)
    private BigDecimal reservedMargin;     // PENDING 매수 예약 증거금 러닝 합 (v1 의 sumReservedMargin 대체)
    private BigDecimal unpaid;             // 미결제 미수금 (B2 는 0)

    public AccountState(long accountId, BigDecimal balance, BigDecimal marginRate) {
        this.accountId = accountId;
        this.balance = balance;
        this.marginRate = marginRate;
        this.reservedMargin = BigDecimal.ZERO;
        this.unpaid = BigDecimal.ZERO;
    }

    /**
     * 이번 매수 주문을 검증하고, 통과하면 예약 증거금을 러닝 합에 더한다.
     *
     * @param orderAmount 주문 금액 (price × quantity)
     * @return 통과면 accepted(예약 증거금), 초과면 rejected(INSUFFICIENT)
     */
    public ReserveResult tryReserve(BigDecimal orderAmount) {
        // 이번 주문에 필요한 예약 증거금 = 주문금액 × 증거금률 (원 단위로 버림)
        BigDecimal reservedThis = orderAmount.multiply(marginRate).setScale(0, RoundingMode.DOWN);

        // 가용 = 잔고 − 기존 예약 증거금 합 − 미결제 미수금
        BigDecimal withdrawable = balance.subtract(reservedMargin).subtract(unpaid);

        // 매수 가능 금액 = 가용 ÷ 증거금률 (원 단위로 버림)
        BigDecimal buyLimit = withdrawable.divide(marginRate, 0, RoundingMode.DOWN);

        if (orderAmount.compareTo(buyLimit) > 0) {
            return ReserveResult.rejected(RejectReason.INSUFFICIENT);
        }

        reservedMargin = reservedMargin.add(reservedThis);
        return ReserveResult.accepted(reservedThis);
    }

    public long accountId() {
        return accountId;
    }

    public BigDecimal reservedMargin() {
        return reservedMargin;
    }
}
