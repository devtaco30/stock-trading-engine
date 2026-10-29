package com.flab.stocktradingengine.account.disruptor.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 종목 slot 이동 중에는 새 매칭 pod가 낸 체결이 먼저 도착하고, 옛 pod의 출력 큐에 남아 있던 더 오래된(더 작은 tradeId)
 * 체결이 늦게 도착할 수 있다(서로 다른 Aeron 연결이라 순서 보장이 없다). 중복 판정은 tradeId 크기를 쓰지 않고 세대별
 * 집합에 들어 있는지만 보며, 가지치기는 세대의 나이로 한다. 이 파일은 그 때문에 「나중에 도착한 작은 tradeId」가
 * 정확히 한 번 반영되는지를 확인한다.
 */
class AccountStateTradeIdArrivalOrderTest {

    private static final String STOCK = "005930";
    private static final BigDecimal PRICE = new BigDecimal("10000");
    private static final long LARGE_TRADE_ID = 900L;
    private static final long SMALL_TRADE_ID = 100L;

    private AccountState stateWithBuyReservation(int quantity) {
        AccountState state = new AccountState(1L, new BigDecimal("100000000"), new BigDecimal("0.40"));
        state.tryReserve(1L, PRICE, quantity);
        return state;
    }

    @Test
    @DisplayName("더 큰 tradeId 체결이 먼저, 더 작은 tradeId 체결이 나중에 도착해도 둘 다 한 번씩 반영되고 재도착은 모두 무시된다")
    void 큰_tradeId가_먼저_와도_작은_tradeId는_한_번_반영된다() {
        AccountState state = stateWithBuyReservation(10);

        assertTrue(state.applyBuyFill(LARGE_TRADE_ID, 1L, STOCK, PRICE, 4).applied());
        assertTrue(state.applyBuyFill(SMALL_TRADE_ID, 1L, STOCK, PRICE, 6).applied(), "나중에 도착한 작은 tradeId가 이미 처리한 것으로 오판됐다");

        assertEquals(10, state.holding(STOCK));
        assertEquals(0, state.reservedMargin().compareTo(BigDecimal.ZERO));
        BigDecimal balanceAfterBoth = state.balance();

        assertFalse(state.applyBuyFill(LARGE_TRADE_ID, 1L, STOCK, PRICE, 4).applied());
        assertFalse(state.applyBuyFill(SMALL_TRADE_ID, 1L, STOCK, PRICE, 6).applied());
        assertEquals(10, state.holding(STOCK), "재도착이 중복 반영됐다");
        assertEquals(0, state.balance().compareTo(balanceAfterBoth));
    }

    @Test
    @DisplayName("매도 체결도 큰 tradeId가 먼저, 작은 tradeId가 나중에 와도 둘 다 한 번씩 반영된다")
    void 매도도_큰_tradeId가_먼저_와도_작은_tradeId는_한_번_반영된다() {
        AccountState state = new AccountState(1L, new BigDecimal("100000000"), new BigDecimal("0.40"),
            java.util.Map.of(STOCK, 10));
        state.trySellReserve(1L, STOCK, 10);

        assertTrue(state.applySellFill(LARGE_TRADE_ID, 1L, STOCK, 4));
        assertTrue(state.applySellFill(SMALL_TRADE_ID, 1L, STOCK, 6), "나중에 도착한 작은 tradeId가 이미 처리한 것으로 오판됐다");
        assertEquals(0, state.holding(STOCK));

        assertFalse(state.applySellFill(LARGE_TRADE_ID, 1L, STOCK, 4));
        assertFalse(state.applySellFill(SMALL_TRADE_ID, 1L, STOCK, 6));
        assertEquals(0, state.holding(STOCK));
    }

    @Test
    @DisplayName("세대 경계 직전(가지치기 전): 큰 tradeId가 옛 세대에, 작은 tradeId가 새 세대에 있어도 둘 다 재도착이 무시된다")
    void 가지치기_직전에는_두_세대의_tradeId가_모두_중복으로_걸러진다() {
        AccountState state = stateWithBuyReservation(10);
        assertTrue(state.applyBuyFill(LARGE_TRADE_ID, 1L, STOCK, PRICE, 4).applied());
        state.startNewGeneration(10_000L);
        assertTrue(state.applyBuyFill(SMALL_TRADE_ID, 1L, STOCK, PRICE, 6).applied());
        state.startNewGeneration(20_000L); // 현재 세대 + 직전 세대 + 가장 오래된 세대, 아직 가지치기 전

        assertFalse(state.applyBuyFill(LARGE_TRADE_ID, 1L, STOCK, PRICE, 4).applied());
        assertFalse(state.applyBuyFill(SMALL_TRADE_ID, 1L, STOCK, PRICE, 6).applied());
        assertEquals(10, state.holding(STOCK));
    }

    @Test
    @DisplayName("가지치기 직후: 큰 tradeId가 있던 옛 세대가 지워진 뒤 처음 도착한 작은 tradeId는 한 번 반영되고, 그 재도착은 무시된다")
    void 가지치기_직후_처음_도착한_작은_tradeId는_한_번_반영된다() {
        AccountState state = stateWithBuyReservation(10);
        assertTrue(state.applyBuyFill(LARGE_TRADE_ID, 1L, STOCK, PRICE, 4).applied()); // 세대 0
        state.startNewGeneration(10_000L);
        state.startNewGeneration(20_000L);
        state.pruneOlderThan(2); // 큰 tradeId가 있던 세대 0이 지워진다

        assertTrue(state.applyBuyFill(SMALL_TRADE_ID, 1L, STOCK, PRICE, 6).applied(), "가지치기 직후 작은 tradeId가 반영되지 않았다");
        assertEquals(10, state.holding(STOCK));
        assertFalse(state.applyBuyFill(SMALL_TRADE_ID, 1L, STOCK, PRICE, 6).applied());
        assertEquals(10, state.holding(STOCK));
    }

    @Test
    @DisplayName("현재 동작 기록: 가지치기는 tradeId 크기가 아니라 세대 나이로 지우므로, 지워진 세대의 큰 tradeId는 재전송되면 다시 반영된다(유한한 중복 창)")
    void 지워진_세대의_tradeId는_크기와_무관하게_다시_반영된다() {
        // 예약 수량을 넉넉히 둬서 재전송이 예외 없이 반영 경로까지 가게 한다.
        AccountState state = stateWithBuyReservation(20);
        assertTrue(state.applyBuyFill(LARGE_TRADE_ID, 1L, STOCK, PRICE, 4).applied()); // 세대 0
        state.startNewGeneration(10_000L);
        assertTrue(state.applyBuyFill(SMALL_TRADE_ID, 1L, STOCK, PRICE, 6).applied()); // 세대 1
        state.startNewGeneration(20_000L);
        state.pruneOlderThan(2); // 세대 0(큰 tradeId)만 지워지고 세대 1(작은 tradeId)은 남는다

        assertFalse(state.applyBuyFill(SMALL_TRADE_ID, 1L, STOCK, PRICE, 6).applied(), "남아 있는 세대의 작은 tradeId는 무시돼야 한다");
        assertTrue(state.applyBuyFill(LARGE_TRADE_ID, 1L, STOCK, PRICE, 4).applied(),
            "지워진 세대의 tradeId 재전송은 현재 구조에서 다시 반영된다 — 이 동작이 달라지면 이 시험이 알려 준다");
        assertEquals(14, state.holding(STOCK)); // 4 + 6 + (재전송 4)
    }
}
