package com.flab.stocktradingengine.account.disruptor.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 여러 recording을 순서 보장 없이 되읽는 복구(I1 U3)가 안전하려면, 체결 반영이 도착 순서와
 * 무관하게 같은 결과를 내야 한다(I1 LLD 3절 D3). 이 성질은 지금 코드 주석에만 있고 테스트가
 * 없었다 — 이 파일이 그 가정을 테스트로 고정한다. 깨지면 U1~U3 설계 전체를 다시 짜야 한다.
 */
class AccountFillOrderIndependenceTest {

    private static final String STOCK = "005930";

    @Test
    @DisplayName("매수 부분체결 3건을 도착순·역순·뒤섞은 순서로 적용해도 잔고·보유·예약잔량·미수금이 모두 같다")
    void 매수_부분체결_적용순서_무관하게_결과가_같다() {
        // 101 × 10 × 0.45 = 454.5 — 반올림이 매 단계 끼어드는 조합을 일부러 골랐다.
        List<Integer> arrival = List.of(2, 5, 3);
        List<Integer> reverse = List.of(3, 5, 2);
        List<Integer> shuffled = List.of(5, 2, 3);

        AccountState arrivalState = applyBuyFillsInOrder(arrival);
        AccountState reverseState = applyBuyFillsInOrder(reverse);
        AccountState shuffledState = applyBuyFillsInOrder(shuffled);

        assertEquals(0, arrivalState.balance().compareTo(reverseState.balance()), "역순 적용 잔고가 도착순과 달라졌다");
        assertEquals(0, arrivalState.balance().compareTo(shuffledState.balance()), "뒤섞은 순서 적용 잔고가 도착순과 달라졌다");
        assertEquals(0, arrivalState.unpaid().compareTo(reverseState.unpaid()), "역순 적용 미수금이 도착순과 달라졌다");
        assertEquals(0, arrivalState.unpaid().compareTo(shuffledState.unpaid()), "뒤섞은 순서 적용 미수금이 도착순과 달라졌다");
        assertEquals(arrivalState.holding(STOCK), reverseState.holding(STOCK), "역순 적용 보유가 도착순과 달라졌다");
        assertEquals(arrivalState.holding(STOCK), shuffledState.holding(STOCK), "뒤섞은 순서 적용 보유가 도착순과 달라졌다");

        // 경계: 마지막 체결로 잔량이 0이 되어 예약 엔트리가 제거되는 경로가 세 순서 모두에서 밟혀야 한다.
        assertEquals(0, arrivalState.reservedMargin().compareTo(BigDecimal.ZERO), "도착순 적용 후 예약이 완전히 소진되지 않았다");
        assertEquals(0, reverseState.reservedMargin().compareTo(BigDecimal.ZERO), "역순 적용 후 예약이 완전히 소진되지 않았다");
        assertEquals(0, shuffledState.reservedMargin().compareTo(BigDecimal.ZERO), "뒤섞은 순서 적용 후 예약이 완전히 소진되지 않았다");
    }

    @Test
    @DisplayName("매도 부분체결 3건을 도착순·뒤섞은 순서로 적용해도 보유·매도예약잔량이 같다")
    void 매도_부분체결_적용순서_무관하게_결과가_같다() {
        List<Integer> arrival = List.of(2, 5, 3);
        List<Integer> shuffled = List.of(5, 2, 3);

        AccountState arrivalState = applySellFillsInOrder(arrival);
        AccountState shuffledState = applySellFillsInOrder(shuffled);

        assertEquals(arrivalState.holding(STOCK), shuffledState.holding(STOCK), "뒤섞은 순서 적용 보유가 도착순과 달라졌다");

        // 경계: 마지막 체결로 잔량이 0이 되어 매도 예약 엔트리가 제거되는 경로가 두 순서 모두에서 밟혀야 한다.
        assertEquals(0, arrivalState.reservedSellQuantity(STOCK), "도착순 적용 후 매도 예약이 완전히 소진되지 않았다");
        assertEquals(0, shuffledState.reservedSellQuantity(STOCK), "뒤섞은 순서 적용 후 매도 예약이 완전히 소진되지 않았다");
    }

    private static AccountState applyBuyFillsInOrder(List<Integer> fillQuantities) {
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("0.45"));
        state.tryReserve(1L, new BigDecimal("101"), 10);

        long tradeId = 9001L;
        for (int fillQty : fillQuantities) {
            state.applyBuyFill(tradeId++, 1L, STOCK, new BigDecimal("101"), fillQty);
        }
        return state;
    }

    private static AccountState applySellFillsInOrder(List<Integer> fillQuantities) {
        AccountState state = new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("0.40"), Map.of(STOCK, 10));
        state.trySellReserve(2L, STOCK, 10);

        long tradeId = 9101L;
        for (int fillQty : fillQuantities) {
            state.applySellFill(tradeId++, 2L, STOCK, fillQty);
        }
        return state;
    }
}
