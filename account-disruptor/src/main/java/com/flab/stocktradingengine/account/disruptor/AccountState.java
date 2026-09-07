package com.flab.stocktradingengine.account.disruptor;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 계좌 하나의 인메모리 상태.
 *
 * <p>소비자 스레드 하나만 이 객체를 만진다(single writer) — 그래서 락이 없다.
 * 검증·예약과 체결 반영을 이 객체가 직접 책임한다(Rich Domain).</p>
 *
 * <h3>예약 장부</h3>
 * <p>예약을 {@code orderId → Reservation(price, remainingQuantity)} 목록(장부)으로 든다.
 * 부분 체결이 오면 그 주문의 남은 수량만 줄여야 하므로, 총액이 아니라 가격·잔량을 따로 기억한다.
 * 예약 증거금은 그때그때 {@code price × remainingQuantity × marginRate}로 다시 계산한다
 * (부분 체결마다 누적 차감하지 않고 잔량 기준으로 매번 새로 계산해, 반올림이 누적되지 않는다).
 * 가용 계산에 쓰는 "예약 총합"은 장부 값들의 합이다(v1 {@code sumReservedMargin} 대체).</p>
 */
public final class AccountState {

    private final long accountId;
    private final BigDecimal balance;      // 총 현금 잔액
    private final BigDecimal marginRate;   // 증거금률 (0.40 ~ 1.00, 시드값 전제)
    private final Map<Long, Reservation> reservations = new HashMap<>();         // orderId → 매수 예약(가격·잔량) 장부
    private final Map<Long, SellReservation> sellReservations = new HashMap<>(); // orderId → 매도 예약(종목·잔량) 장부
    private final Map<String, Integer> holdings = new HashMap<>();      // 종목코드 → 보유 수량
    private final Set<Long> processedTradeIds = new HashSet<>();        // 이미 반영한 체결(tradeId), 멱등용
    private BigDecimal unpaid = BigDecimal.ZERO;                        // 미결제 미수금

    public AccountState(long accountId, BigDecimal balance, BigDecimal marginRate) {
        this.accountId = accountId;
        this.balance = balance;
        this.marginRate = marginRate;
    }

    /**
     * 매수 주문을 검증하고, 통과하면 그 주문의 예약(가격·수량)을 장부에 기록한다.
     *
     * @param orderId  주문 신원(장부 키) — 나중에 체결 시 이 키로 예약을 푼다
     * @param price    주문 가격(지정가) — 예약 증거금 계산의 기준
     * @param quantity 주문 수량
     * @return 통과면 accepted(예약 증거금), 초과면 rejected(INSUFFICIENT)
     */
    public ReserveResult tryReserve(long orderId, BigDecimal price, int quantity) {
        BigDecimal orderAmount = price.multiply(BigDecimal.valueOf(quantity));
        BigDecimal reservedThis = reservedAmount(price, quantity);
        BigDecimal withdrawable = balance.subtract(totalReserved()).subtract(unpaid);
        BigDecimal buyLimit = withdrawable.divide(marginRate, 0, RoundingMode.DOWN);

        if (orderAmount.compareTo(buyLimit) > 0) {
            return ReserveResult.rejected(RejectReason.INSUFFICIENT);
        }

        reservations.put(orderId, new Reservation(price, quantity));
        return ReserveResult.accepted(reservedThis);
    }

    /**
     * 매수 체결 반영(전량·부분 공통): 체결된 수량만큼 그 주문의 예약을 줄이고, 보유를 늘리고,
     * 미수금을 만든다. 잔량이 남으면 예약을 유지하고, 0이 되면 장부에서 지운다.
     * (balance 는 안 뺀다 — 나머지는 미수금으로 T+2 결제.)
     *
     * @param tradeId 체결 신원(멱등키) — 이미 반영한 tradeId 면 아무것도 하지 않고 무시한다
     * @throws IllegalStateException 그 orderId 로 예약된 게 없거나, 체결 수량이 남은 예약 수량을 초과하면
     *                                (매칭이 검증 안 된 주문을 체결시킨 것이므로 도메인 불변식 위반)
     * @return 이번 호출로 실제 반영했으면 true, 이미 반영한 tradeId 라 무시했으면 false
     */
    public boolean applyBuyFill(long tradeId, long orderId, String stockCode, BigDecimal matchPrice, int fillQty) {
        if (!processedTradeIds.add(tradeId)) {
            return false; // 이미 반영한 체결 재도착 — 무시
        }
        Reservation reservation = reservations.get(orderId);
        if (reservation == null) {
            throw new IllegalStateException("예약되지 않은 주문에 대한 체결입니다: orderId=" + orderId);
        }
        int remainingQuantity = reservation.remainingQuantity() - fillQty;
        if (remainingQuantity < 0) {
            throw new IllegalStateException("체결 수량이 남은 예약 수량을 초과합니다: orderId=" + orderId
                + " 남은수량=" + reservation.remainingQuantity() + " 체결수량=" + fillQty);
        }
        if (remainingQuantity == 0) {
            reservations.remove(orderId);
        } else {
            reservations.put(orderId, new Reservation(reservation.price(), remainingQuantity));
        }
        // 보유 추가
        holdings.merge(stockCode, fillQty, Integer::sum);
        // 미수금 = 체결액 × (1 − 증거금률). balance 는 안 뺀다 (T+2 결제).
        BigDecimal fillAmount = matchPrice.multiply(BigDecimal.valueOf(fillQty));
        BigDecimal unpaidThis = fillAmount.multiply(BigDecimal.ONE.subtract(marginRate)).setScale(0, RoundingMode.DOWN);
        unpaid = unpaid.add(unpaidThis);
        return true;
    }

    /**
     * 매도 주문을 검증하고, 통과하면 그 주문의 매도 예약(보유 수량 담보)을 장부에 기록한다.
     * 매수의 {@link #tryReserve}와 대칭이지만 담보가 돈이 아니라 보유 수량이다.
     *
     * @param orderId  주문 신원(장부 키) — 나중에 체결 시 이 키로 예약을 푼다
     * @param quantity 매도 수량
     * @return 통과면 accepted(예약 수량), 초과면 rejected(INSUFFICIENT_HOLDING)
     */
    public SellReserveResult trySellReserve(long orderId, String stockCode, int quantity) {
        int available = holding(stockCode) - reservedSellQuantity(stockCode);
        if (quantity > available) {
            return SellReserveResult.rejected(RejectReason.INSUFFICIENT_HOLDING);
        }
        sellReservations.put(orderId, new SellReservation(stockCode, quantity));
        return SellReserveResult.accepted(quantity);
    }

    /**
     * 매도 체결 반영(전량·부분 공통): 체결된 수량만큼 그 주문의 매도 예약을 줄이고 보유를 줄인다.
     * 잔량이 남으면 예약을 유지하고, 0이 되면 장부에서 지운다.
     *
     * @param tradeId 체결 신원(멱등키) — 이미 반영한 tradeId 면 아무것도 하지 않고 무시한다
     * @throws IllegalStateException 그 orderId 로 매도 예약된 게 없거나, 체결 수량이 남은 예약 수량을 초과하면
     * @return 이번 호출로 실제 반영했으면 true, 이미 반영한 tradeId 라 무시했으면 false
     */
    public boolean applySellFill(long tradeId, long orderId, String stockCode, int fillQty) {
        if (!processedTradeIds.add(tradeId)) {
            return false; // 이미 반영한 체결 재도착 — 무시
        }
        SellReservation reservation = sellReservations.get(orderId);
        if (reservation == null) {
            throw new IllegalStateException("매도 예약되지 않은 주문에 대한 체결입니다: orderId=" + orderId);
        }
        int remainingQuantity = reservation.remainingQuantity() - fillQty;
        if (remainingQuantity < 0) {
            throw new IllegalStateException("체결 수량이 남은 매도 예약 수량을 초과합니다: orderId=" + orderId
                + " 남은수량=" + reservation.remainingQuantity() + " 체결수량=" + fillQty);
        }
        if (remainingQuantity == 0) {
            sellReservations.remove(orderId);
        } else {
            sellReservations.put(orderId, new SellReservation(reservation.stockCode(), remainingQuantity));
        }
        holdings.merge(stockCode, -fillQty, Integer::sum);
        return true;
    }

    private BigDecimal totalReserved() {
        BigDecimal sum = BigDecimal.ZERO;
        for (Reservation reservation : reservations.values()) {
            sum = sum.add(reservedAmount(reservation.price(), reservation.remainingQuantity()));
        }
        return sum;
    }

    private BigDecimal reservedAmount(BigDecimal price, int quantity) {
        return price.multiply(BigDecimal.valueOf(quantity)).multiply(marginRate).setScale(0, RoundingMode.DOWN);
    }

    /** 주문 예약 장부 한 줄 — 가격과 남은(미체결) 수량. 예약 증거금은 이 둘로 매번 다시 계산한다. */
    private record Reservation(BigDecimal price, int remainingQuantity) {
    }

    /** 특정 종목의 현재 매도 예약 수량 총합(장부 값의 합). */
    public int reservedSellQuantity(String stockCode) {
        int sum = 0;
        for (SellReservation reservation : sellReservations.values()) {
            if (reservation.stockCode().equals(stockCode)) {
                sum += reservation.remainingQuantity();
            }
        }
        return sum;
    }

    /** 매도 예약 장부 한 줄 — 종목코드와 남은(미체결) 수량. */
    private record SellReservation(String stockCode, int remainingQuantity) {
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
