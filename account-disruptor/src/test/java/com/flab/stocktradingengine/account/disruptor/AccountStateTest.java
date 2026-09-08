package com.flab.stocktradingengine.account.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AccountStateTest {

    private static final String STOCK = "005930";

    // ---------- 초기 보유 시드 ----------

    @Test
    @DisplayName("초기 보유를 시드하면 그 종목의 보유 수량으로 바로 조회된다")
    void 초기보유_시드하면_바로_조회된다() {
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("0.40"),
            Map.of(STOCK, 10));

        assertEquals(10, state.holding(STOCK));
    }

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

    // ---------- requestId 재전송 멱등 + orderId 발급 조회 (C5-1a, C5-2a) ----------

    @Test
    @DisplayName("처음 보는 requestId면 null을 돌려준다")
    void 처음보는requestId_null() {
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));

        assertNull(state.orderIdFor("r1"));
    }

    @Test
    @DisplayName("기억한 requestId는 발급했던 orderId를 그대로 돌려준다")
    void 기억한requestId_발급했던orderId_반환() {
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        state.rememberRequest("r1", 42L);

        assertEquals(42L, state.orderIdFor("r1"));
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
        // 잔고 = 시드 − 증거금분(체결액 × 증거금률 = 100000 × 0.40 = 40000)
        assertEquals(0, state.balance().compareTo(new BigDecimal("960000")));
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
        // 잔고도 한 번만 깎여야 한다(둘 다 반영되면 920000이 됨)
        assertEquals(0, state.balance().compareTo(new BigDecimal("960000")));
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
        // 잔고 = 시드 − 이번 체결분 증거금(40000 × 0.40 = 16000)
        assertEquals(0, state.balance().compareTo(new BigDecimal("984000")));
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
        // 두 부분체결의 증거금분이 누적 차감됨 = 전체 체결액(100000) × 0.40 = 40000
        assertEquals(0, state.balance().compareTo(new BigDecimal("960000")));
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

    // ---------- 미수금 발행 트리거 (a2-1) ----------

    @Test
    @DisplayName("매수 체결이 반영되면 이번 체결로 새로 생긴 미수금을 결과로 돌려준다")
    void 매수체결_결과로_이번_미수금을_돌려준다() {
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        state.tryReserve(1L, new BigDecimal("10000"), 10);

        BuyFillResult result = state.applyBuyFill(101L, 1L, STOCK, new BigDecimal("10000"), 10);

        assertTrue(result.applied());
        // 미수금 = 체결액 × (1 − 증거금률) = 100000 × 0.60 = 60000
        assertEquals(0, result.unpaidThis().compareTo(new BigDecimal("60000")));
    }

    @Test
    @DisplayName("같은 tradeId로 매수 체결이 두 번 오면 둘째 결과는 applied=false, unpaidThis=0이다")
    void 매수체결_같은tradeId_둘째결과는_미적용() {
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        state.tryReserve(1L, new BigDecimal("10000"), 10);
        state.applyBuyFill(101L, 1L, STOCK, new BigDecimal("10000"), 10);

        BuyFillResult result = state.applyBuyFill(101L, 1L, STOCK, new BigDecimal("10000"), 10); // 같은 tradeId 재도착

        assertFalse(result.applied());
        assertEquals(0, result.unpaidThis().compareTo(BigDecimal.ZERO));
    }

    @Test
    @DisplayName("부분 체결의 미수금 결과는 이번 체결분만이다 (누적이 아니다)")
    void 부분체결_미수금결과는_이번체결분만() {
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        state.tryReserve(1L, new BigDecimal("10000"), 10); // 10주 예약

        BuyFillResult first = state.applyBuyFill(101L, 1L, STOCK, new BigDecimal("10000"), 4); // 4주 체결
        BuyFillResult second = state.applyBuyFill(102L, 1L, STOCK, new BigDecimal("10000"), 6); // 나머지 6주 체결

        // 1차 미수금 = 40000 × 0.60 = 24000, 2차 미수금 = 60000 × 0.60 = 36000 (누적 아님)
        assertEquals(0, first.unpaidThis().compareTo(new BigDecimal("24000")));
        assertEquals(0, second.unpaidThis().compareTo(new BigDecimal("36000")));
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

    // ---------- 정산 되돌림 (settlement, a1) ----------

    @Test
    @DisplayName("정산이 오면 잔고를 깎고 미수금을 줄인다")
    void 정산오면_잔고깎고_미수금줄인다() {
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        state.tryReserve(1L, new BigDecimal("10000"), 10);
        state.applyBuyFill(101L, 1L, STOCK, new BigDecimal("10000"), 10); // 미수금 60000 생김

        boolean applied = state.applySettlement(9001L, new BigDecimal("60000"));

        assertTrue(applied);
        // 체결 때 증거금분(40000) 먼저 깎이고, 정산이 미수금분(60000) 마저 깎아 체결액 전액(100000)이 빠진다
        assertEquals(0, state.balance().compareTo(new BigDecimal("900000"))); // 1000000 - 40000 - 60000
        assertEquals(0, state.unpaid().compareTo(BigDecimal.ZERO));
    }

    @Test
    @DisplayName("같은 settlementRef가 다시 오면 무시하고 잔고를 두 번 안 깎는다")
    void 같은settlementRef_재도착하면_무시() {
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        state.tryReserve(1L, new BigDecimal("10000"), 10);
        state.applyBuyFill(101L, 1L, STOCK, new BigDecimal("10000"), 10);

        state.applySettlement(9001L, new BigDecimal("60000"));
        boolean secondApplied = state.applySettlement(9001L, new BigDecimal("60000")); // 같은 settlementRef 재도착

        assertFalse(secondApplied);
        assertEquals(0, state.balance().compareTo(new BigDecimal("900000"))); // 정산은 한 번만 깎임(체결분 40000 + 정산분 60000)
        assertEquals(0, state.unpaid().compareTo(BigDecimal.ZERO));
    }

    @Test
    @DisplayName("정산 금액이 미수금을 초과하면 예외를 던진다")
    void 정산금액이_미수금초과하면_예외() {
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        state.tryReserve(1L, new BigDecimal("10000"), 10);
        state.applyBuyFill(101L, 1L, STOCK, new BigDecimal("10000"), 10); // 미수금 60000

        assertThrows(IllegalStateException.class,
            () -> state.applySettlement(9001L, new BigDecimal("60001")));
    }

    // ---------- 부분체결 반올림 누적 (Jack 지적: 리뷰가 "무시해도 됨"이라 했던 것 재검증) ----------

    @Test
    @DisplayName("가격×수량×증거금률이 딱 안 떨어져도, 부분체결 두 번의 증거금 합은 최초 예약액과 원 단위까지 정확히 같다")
    void 부분체결_증거금합이_최초예약액과_정확히_같다() {
        // 101 × 10 × 0.45 = 454.5 — 정수로 안 떨어지는 조합을 일부러 골랐다.
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("0.45"));
        ReserveResult reserveResult = state.tryReserve(1L, new BigDecimal("101"), 10);
        BigDecimal initialReservedMargin = reserveResult.reservedMargin();

        state.applyBuyFill(101L, 1L, STOCK, new BigDecimal("101"), 3); // 3주 체결, 잔량 7
        state.applyBuyFill(102L, 1L, STOCK, new BigDecimal("101"), 7); // 나머지 7주 체결, 잔량 0

        BigDecimal totalMarginPaid = new BigDecimal("1000000").subtract(state.balance());
        // 두 부분체결이 balance에서 뺀 증거금 합이, 애초에 예약했던 금액과 원 단위까지 정확히 같아야 한다.
        // 어긋나면 "검증(tryReserve)한 적 없는 금액"이 조용히 더 빠져나간 것이다(회계 사고).
        assertEquals(0, totalMarginPaid.compareTo(initialReservedMargin));
        // 증거금 + 미수금 = 체결액 전액(101 × 10 = 1010)이어야 한다.
        assertEquals(0, totalMarginPaid.add(state.unpaid()).compareTo(new BigDecimal("1010")));
    }
}
