package com.flab.stocktradingengine.account.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AccountStateTest {

    private static final String STOCK = "005930";

    // ---------- 검증·예약 (B2) ----------

    @Test
    @DisplayName("가용 금액이 충분하면 매수를 통과시키고 예약 증거금을 반환한다")
    void 충분하면_통과하고_예약증거금_반환() {
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));

        ReserveResult result = state.tryReserve(1L, new BigDecimal("100000")); // 10000 × 10

        assertTrue(result.accepted());
        // 예약 증거금 = 주문금액 × 증거금률 = 100000 × 0.40 = 40000
        assertEquals(0, result.reservedMargin().compareTo(new BigDecimal("40000")));
    }

    @Test
    @DisplayName("주문 금액이 매수 가능 금액을 넘으면 거부하고 상태를 바꾸지 않는다")
    void 초과하면_거부하고_상태불변() {
        AccountState state = new AccountState(1L, new BigDecimal("30000"), new BigDecimal("0.40"));

        // buyLimit = 가용(30000) ÷ 0.40 = 75000, 주문 100000 > 75000 → 거부
        ReserveResult result = state.tryReserve(1L, new BigDecimal("100000"));

        assertFalse(result.accepted());
        assertEquals(RejectReason.INSUFFICIENT, result.reason());
        assertEquals(0, state.reservedMargin().compareTo(BigDecimal.ZERO));
    }

    @Test
    @DisplayName("연속 주문에서 둘째는 첫째의 예약을 뺀 가용으로 검증된다 (장부 합)")
    void 러닝예약_둘째는_첫째_예약뺀_가용으로_검증() {
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("1.00"));

        ReserveResult first = state.tryReserve(1L, new BigDecimal("600000"));  // 가용 1,000,000 → 통과
        ReserveResult second = state.tryReserve(2L, new BigDecimal("600000")); // 가용 400,000 → 거부

        assertTrue(first.accepted());
        assertFalse(second.accepted());
        assertEquals(RejectReason.INSUFFICIENT, second.reason());
    }

    @Test
    @DisplayName("매수 가능 금액과 딱 같으면 통과, 1 초과면 거부한다 (경계)")
    void 경계_딱은_통과_한칸_초과는_거부() {
        AccountState atLimit = new AccountState(1L, new BigDecimal("100000"), new BigDecimal("1.00"));
        AccountState overLimit = new AccountState(2L, new BigDecimal("100000"), new BigDecimal("1.00"));

        assertTrue(atLimit.tryReserve(10L, new BigDecimal("100000")).accepted());
        assertFalse(overLimit.tryReserve(20L, new BigDecimal("100001")).accepted());
    }

    // ---------- 체결 반영 (B3a, 전량) ----------

    @Test
    @DisplayName("매수 전량 체결: 그 주문 예약을 풀고, 보유를 늘리고, 미수금을 만든다")
    void 매수체결_예약풀고_보유늘고_미수금생성() {
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        state.tryReserve(1L, new BigDecimal("100000")); // 예약 40000

        state.applyBuyFill(1L, STOCK, new BigDecimal("10000"), 10);

        // 예약 풀림
        assertEquals(0, state.reservedMargin().compareTo(BigDecimal.ZERO));
        // 보유 증가
        assertEquals(10, state.holding(STOCK));
        // 미수금 = 체결액 × (1 − 증거금률) = 100000 × 0.60 = 60000
        assertEquals(0, state.unpaid().compareTo(new BigDecimal("60000")));
    }

    @Test
    @DisplayName("매도 전량 체결: 보유를 줄인다")
    void 매도체결_보유감소() {
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        state.tryReserve(1L, new BigDecimal("100000"));
        state.applyBuyFill(1L, STOCK, new BigDecimal("10000"), 10); // 보유 10 확보

        state.applySellFill(STOCK, 4);

        assertEquals(6, state.holding(STOCK));
    }
}
