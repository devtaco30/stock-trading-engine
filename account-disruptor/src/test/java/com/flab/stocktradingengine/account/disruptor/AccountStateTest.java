package com.flab.stocktradingengine.account.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

        ReserveResult result = state.tryReserve(1L, new BigDecimal("10000"), 10); // 10000 × 10

        assertTrue(result.accepted());
        // 예약 증거금 = 주문금액 × 증거금률 = 100000 × 0.40 = 40000
        assertEquals(0, result.reservedMargin().compareTo(new BigDecimal("40000")));
    }

    @Test
    @DisplayName("주문 금액이 매수 가능 금액을 넘으면 거부하고 상태를 바꾸지 않는다")
    void 초과하면_거부하고_상태불변() {
        AccountState state = new AccountState(1L, new BigDecimal("30000"), new BigDecimal("0.40"));

        // buyLimit = 가용(30000) ÷ 0.40 = 75000, 주문 100000 > 75000 → 거부
        ReserveResult result = state.tryReserve(1L, new BigDecimal("10000"), 10);

        assertFalse(result.accepted());
        assertEquals(RejectReason.INSUFFICIENT, result.reason());
        assertEquals(0, state.reservedMargin().compareTo(BigDecimal.ZERO));
    }

    @Test
    @DisplayName("연속 주문에서 둘째는 첫째의 예약을 뺀 가용으로 검증된다 (장부 합)")
    void 러닝예약_둘째는_첫째_예약뺀_가용으로_검증() {
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("1.00"));

        ReserveResult first = state.tryReserve(1L, new BigDecimal("60000"), 10);  // 600000, 가용 1,000,000 → 통과
        ReserveResult second = state.tryReserve(2L, new BigDecimal("60000"), 10); // 가용 400,000 → 거부

        assertTrue(first.accepted());
        assertFalse(second.accepted());
        assertEquals(RejectReason.INSUFFICIENT, second.reason());
    }

    @Test
    @DisplayName("매수 가능 금액과 딱 같으면 통과, 1 초과면 거부한다 (경계)")
    void 경계_딱은_통과_한칸_초과는_거부() {
        AccountState atLimit = new AccountState(1L, new BigDecimal("100000"), new BigDecimal("1.00"));
        AccountState overLimit = new AccountState(2L, new BigDecimal("100000"), new BigDecimal("1.00"));

        assertTrue(atLimit.tryReserve(10L, new BigDecimal("100000"), 1).accepted());
        assertFalse(overLimit.tryReserve(20L, new BigDecimal("100001"), 1).accepted());
    }

    // ---------- 체결 반영 (B3a, 전량) ----------

    @Test
    @DisplayName("매수 전량 체결: 그 주문 예약을 풀고, 보유를 늘리고, 미수금을 만든다")
    void 매수체결_예약풀고_보유늘고_미수금생성() {
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        state.tryReserve(1L, new BigDecimal("10000"), 10); // 예약 40000

        state.applyBuyFill(101L, 1L, STOCK, new BigDecimal("10000"), 10);

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
        state.tryReserve(1L, new BigDecimal("10000"), 10);
        state.applyBuyFill(101L, 1L, STOCK, new BigDecimal("10000"), 10); // 보유 10 확보
        state.trySellReserve(2L, STOCK, 4);

        state.applySellFill(102L, 2L, STOCK, 4);

        assertEquals(6, state.holding(STOCK));
    }

    // ---------- tradeId 멱등 (B3b) ----------

    @Test
    @DisplayName("같은 tradeId로 매수 체결이 두 번 오면 둘째는 무시한다 (보유·미수금 불변)")
    void 매수체결_같은tradeId_둘째는_무시() {
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        state.tryReserve(1L, new BigDecimal("10000"), 10);

        state.applyBuyFill(101L, 1L, STOCK, new BigDecimal("10000"), 10);
        state.applyBuyFill(101L, 1L, STOCK, new BigDecimal("10000"), 10); // 같은 tradeId 재도착

        assertEquals(10, state.holding(STOCK));
        assertEquals(0, state.unpaid().compareTo(new BigDecimal("60000")));
    }

    @Test
    @DisplayName("같은 tradeId로 매도 체결이 두 번 오면 둘째는 무시한다 (보유 불변)")
    void 매도체결_같은tradeId_둘째는_무시() {
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        state.tryReserve(1L, new BigDecimal("10000"), 10);
        state.applyBuyFill(101L, 1L, STOCK, new BigDecimal("10000"), 10); // 보유 10 확보
        state.trySellReserve(2L, STOCK, 4);

        state.applySellFill(102L, 2L, STOCK, 4);
        state.applySellFill(102L, 2L, STOCK, 4); // 같은 tradeId 재도착

        assertEquals(6, state.holding(STOCK));
    }

    // ---------- 부분 체결 (B3c) ----------

    @Test
    @DisplayName("부분 체결: 체결된 수량만큼만 예약을 줄이고, 남은 수량은 계속 예약해 둔다")
    void 부분체결_체결분만큼만_예약감소() {
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        state.tryReserve(1L, new BigDecimal("10000"), 10); // 예약 40000 (10주)

        state.applyBuyFill(101L, 1L, STOCK, new BigDecimal("10000"), 4); // 4주만 체결

        // 남은 예약 = 10000 × (10-4) × 0.40 = 24000
        assertEquals(0, state.reservedMargin().compareTo(new BigDecimal("24000")));
        assertEquals(4, state.holding(STOCK));
        // 미수금 = 체결액 × (1 − 증거금률) = 40000 × 0.60 = 24000
        assertEquals(0, state.unpaid().compareTo(new BigDecimal("24000")));
    }

    @Test
    @DisplayName("부분 체결이 이어져 잔량이 0이 되면 예약을 완전히 지운다")
    void 부분체결_잔량0되면_예약완전소진() {
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        state.tryReserve(1L, new BigDecimal("10000"), 10); // 예약 40000 (10주)

        state.applyBuyFill(101L, 1L, STOCK, new BigDecimal("10000"), 4); // 4주 체결, 잔량 6
        state.applyBuyFill(102L, 1L, STOCK, new BigDecimal("10000"), 6); // 나머지 6주 체결, 잔량 0

        assertEquals(0, state.reservedMargin().compareTo(BigDecimal.ZERO));
        assertEquals(10, state.holding(STOCK));
    }

    @Test
    @DisplayName("체결 수량이 남은 예약 수량을 초과하면 예외를 던진다")
    void 체결수량_남은예약초과하면_예외() {
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        state.tryReserve(1L, new BigDecimal("10000"), 10); // 10주 예약

        assertThrows(IllegalStateException.class,
            () -> state.applyBuyFill(101L, 1L, STOCK, new BigDecimal("10000"), 11));
    }

    @Test
    @DisplayName("예약이 없는 주문에 체결이 오면 예외를 던진다")
    void 예약없는주문에_체결오면_예외() {
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));

        assertThrows(IllegalStateException.class,
            () -> state.applyBuyFill(101L, 999L, STOCK, new BigDecimal("10000"), 1));
    }

    // ---------- 매도 보유예약 ----------

    private static AccountState withHolding(int quantity) {
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        state.tryReserve(1L, new BigDecimal("10000"), quantity);
        state.applyBuyFill(101L, 1L, STOCK, new BigDecimal("10000"), quantity);
        return state;
    }

    @Test
    @DisplayName("보유 수량 안이면 매도 예약을 통과시킨다")
    void 매도예약_보유안이면_통과() {
        AccountState state = withHolding(10);

        SellReserveResult result = state.trySellReserve(2L, STOCK, 4);

        assertTrue(result.accepted());
        assertEquals(4, result.reservedQuantity());
    }

    @Test
    @DisplayName("보유 수량을 넘는 매도는 INSUFFICIENT_HOLDING 으로 거부한다")
    void 매도예약_보유초과하면_거부() {
        AccountState state = withHolding(10);

        SellReserveResult result = state.trySellReserve(2L, STOCK, 11);

        assertFalse(result.accepted());
        assertEquals(RejectReason.INSUFFICIENT_HOLDING, result.reason());
    }

    @Test
    @DisplayName("연속 매도 주문에서 둘째는 첫째의 예약을 뺀 가용으로 검증된다 (장부 합)")
    void 매도_러닝예약_둘째는_첫째_예약뺀_가용으로_검증() {
        AccountState state = withHolding(10);

        SellReserveResult first = state.trySellReserve(2L, STOCK, 6);  // 가용 10 → 통과, 남은 가용 4
        SellReserveResult second = state.trySellReserve(3L, STOCK, 6); // 가용 4 → 거부

        assertTrue(first.accepted());
        assertFalse(second.accepted());
        assertEquals(RejectReason.INSUFFICIENT_HOLDING, second.reason());
    }

    @Test
    @DisplayName("부분 매도 체결: 체결된 수량만큼만 예약을 줄이고, 남은 수량은 계속 예약해 둔다")
    void 매도_부분체결_체결분만큼만_예약감소() {
        AccountState state = withHolding(10);
        state.trySellReserve(2L, STOCK, 10); // 10주 전부 예약

        state.applySellFill(102L, 2L, STOCK, 4); // 4주만 체결

        assertEquals(6, state.reservedSellQuantity(STOCK));
        assertEquals(6, state.holding(STOCK)); // 10 - 4
    }

    @Test
    @DisplayName("부분 매도 체결이 이어져 잔량이 0이 되면 예약을 완전히 지운다")
    void 매도_부분체결_잔량0되면_예약완전소진() {
        AccountState state = withHolding(10);
        state.trySellReserve(2L, STOCK, 10);

        state.applySellFill(102L, 2L, STOCK, 4); // 잔량 6
        state.applySellFill(103L, 2L, STOCK, 6); // 잔량 0

        assertEquals(0, state.reservedSellQuantity(STOCK));
        assertEquals(0, state.holding(STOCK));
    }

    @Test
    @DisplayName("매도 체결 수량이 남은 예약 수량을 초과하면 예외를 던진다")
    void 매도_체결수량_남은예약초과하면_예외() {
        AccountState state = withHolding(10);
        state.trySellReserve(2L, STOCK, 5);

        assertThrows(IllegalStateException.class,
            () -> state.applySellFill(102L, 2L, STOCK, 6));
    }

    @Test
    @DisplayName("예약이 없는 매도 주문에 체결이 오면 예외를 던진다")
    void 매도_예약없는주문에_체결오면_예외() {
        AccountState state = withHolding(10);

        assertThrows(IllegalStateException.class,
            () -> state.applySellFill(102L, 999L, STOCK, 1));
    }
}
