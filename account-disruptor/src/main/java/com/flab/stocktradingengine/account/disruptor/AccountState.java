package com.flab.stocktradingengine.account.disruptor;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

/**
 * 계좌 하나의 인메모리 상태.
 *
 * <p>소비자 스레드 하나만 이 객체를 만진다(single writer) — 그래서 락이 없다.
 * 검증·예약과 체결 반영을 이 객체가 직접 책임한다(Rich Domain).</p>
 *
 * <h3>예약 장부</h3>
 * <p>예약 증거금을 총합 숫자 하나가 아니라 {@code orderId → 증거금} 목록(장부)으로 든다.
 * 체결이 나면 그 주문의 예약만 콕 집어 풀어야 하므로, 주문별로 나눠 기억한다.
 * 가용 계산에 쓰는 "예약 총합"은 장부 값의 합이다(v1 {@code sumReservedMargin} 대체).</p>
 */
public final class AccountState {

    private final long accountId;
    private final BigDecimal balance;      // 총 현금 잔액
    private final BigDecimal marginRate;   // 증거금률 (0.40 ~ 1.00, 시드값 전제)
    private final Map<Long, BigDecimal> reservations = new HashMap<>(); // orderId → 예약 증거금 (장부)
    private final Map<String, Integer> holdings = new HashMap<>();      // 종목코드 → 보유 수량
    private BigDecimal unpaid = BigDecimal.ZERO;                        // 미결제 미수금

    public AccountState(long accountId, BigDecimal balance, BigDecimal marginRate) {
        this.accountId = accountId;
        this.balance = balance;
        this.marginRate = marginRate;
    }

    /**
     * 매수 주문을 검증하고, 통과하면 그 주문의 예약 증거금을 장부에 기록한다.
     *
     * @param orderId     주문 신원(장부 키) — 나중에 체결 시 이 키로 예약을 푼다
     * @param orderAmount 주문 금액 (price × quantity)
     * @return 통과면 accepted(예약 증거금), 초과면 rejected(INSUFFICIENT)
     */
    public ReserveResult tryReserve(long orderId, BigDecimal orderAmount) {
        BigDecimal reservedThis = orderAmount.multiply(marginRate).setScale(0, RoundingMode.DOWN);
        BigDecimal withdrawable = balance.subtract(totalReserved()).subtract(unpaid);
        BigDecimal buyLimit = withdrawable.divide(marginRate, 0, RoundingMode.DOWN);

        if (orderAmount.compareTo(buyLimit) > 0) {
            return ReserveResult.rejected(RejectReason.INSUFFICIENT);
        }

        reservations.put(orderId, reservedThis);
        return ReserveResult.accepted(reservedThis);
    }

    /**
     * 매수 전량 체결 반영: 그 주문의 예약을 풀고, 보유를 늘리고, 미수금을 만든다.
     * (balance 는 안 뺀다 — 나머지는 미수금으로 T+2 결제.)
     */
    public void applyBuyFill(long orderId, String stockCode, BigDecimal matchPrice, int fillQty) {
        // 예약 풀기 (전량이므로 그 주문 줄을 통째로 지운다)
        reservations.remove(orderId);
        // 보유 추가
        holdings.merge(stockCode, fillQty, Integer::sum);
        // 미수금 = 체결액 × (1 − 증거금률). balance 는 안 뺀다 (T+2 결제).
        BigDecimal fillAmount = matchPrice.multiply(BigDecimal.valueOf(fillQty));
        BigDecimal unpaidThis = fillAmount.multiply(BigDecimal.ONE.subtract(marginRate)).setScale(0, RoundingMode.DOWN);
        unpaid = unpaid.add(unpaidThis);
    }

    /** 매도 전량 체결 반영: 보유를 줄인다. */
    public void applySellFill(String stockCode, int fillQty) {
        holdings.merge(stockCode, -fillQty, Integer::sum);
    }

    private BigDecimal totalReserved() {
        BigDecimal sum = BigDecimal.ZERO;
        for (BigDecimal reserved : reservations.values()) {
            sum = sum.add(reserved);
        }
        return sum;
    }

    public long accountId() {
        return accountId;
    }

    /** 현재 예약 증거금 총합(장부 값의 합). */
    public BigDecimal reservedMargin() {
        return totalReserved();
    }

    public int holding(String stockCode) {
        return holdings.getOrDefault(stockCode, 0);
    }

    public BigDecimal unpaid() {
        return unpaid;
    }
}
