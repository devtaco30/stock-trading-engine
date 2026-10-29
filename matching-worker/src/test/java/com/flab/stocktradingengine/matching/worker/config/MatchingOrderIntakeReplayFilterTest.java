package com.flab.stocktradingengine.matching.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.function.IntPredicate;

import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.StockSlotHasher;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.matching.worker.recovery.OwnedStockSlotRestorer;
import com.flab.stocktradingengine.trading.entity.OrderSide;

/** 복구 입력(주문 입구 녹화)에서 담당이 아닌 종목 slot의 주문을 걸러내는 정적 메서드를 Archive 없이 확인한다. */
class MatchingOrderIntakeReplayFilterTest {

    private static final StockSlotHasher STOCK_SLOT_HASHER = new StockSlotHasher(64);

    @Test
    void 담당_종목_주문_둘과_담당_아닌_종목_주문_하나를_걸러_담당_둘만_순서대로_남긴다() {
        JournaledOrder owned1 = place(1L, "005930");
        JournaledOrder notOwned = place(2L, "000660");
        JournaledOrder owned2 = place(3L, "005930");
        int ownedSlot = STOCK_SLOT_HASHER.slotFor("005930");
        IntPredicate ownedSlots = slot -> slot == ownedSlot;

        List<JournaledOrder> filtered = MatchingOrderIntakeConfig.filterOwnedEntries(
            List.of(owned1, notOwned, owned2), ownedSlots, STOCK_SLOT_HASHER);

        assertThat(filtered).containsExactly(owned1, owned2);
    }

    @Test
    void 모두_담당이면_그대로_남고_빈_목록은_빈_목록이다() {
        JournaledOrder order = place(1L, "005930");

        assertThat(MatchingOrderIntakeConfig.filterOwnedEntries(List.of(order), slot -> true, STOCK_SLOT_HASHER))
            .containsExactly(order);
        assertThat(MatchingOrderIntakeConfig.filterOwnedEntries(List.of(), slot -> false, STOCK_SLOT_HASHER)).isEmpty();
    }

    @Test
    void 복원한_목록을_걸러내기에_쓰면_담당_밖_주문이_제외된다() {
        int ownedSlot = STOCK_SLOT_HASHER.slotFor("005930");
        int handedOverSlot = STOCK_SLOT_HASHER.slotFor("000660");
        Set<Integer> restored = OwnedStockSlotRestorer.restore(Set.of(ownedSlot, handedOverSlot), List.of(handedOverSlot));
        JournaledOrder owned = place(1L, "005930");
        JournaledOrder handedOver = place(2L, "000660");

        List<JournaledOrder> filtered = MatchingOrderIntakeConfig.filterOwnedEntries(
            List.of(owned, handedOver), restored::contains, STOCK_SLOT_HASHER);

        assertThat(filtered).containsExactly(owned);
    }

    private JournaledOrder place(long orderId, String stockCode) {
        return new JournaledOrder(EventType.PLACE, orderId, 100L + orderId, stockCode, OrderSide.BUY,
            new BigDecimal("70000"), 10, Instant.parse("2026-10-29T00:00:00Z"));
    }
}
