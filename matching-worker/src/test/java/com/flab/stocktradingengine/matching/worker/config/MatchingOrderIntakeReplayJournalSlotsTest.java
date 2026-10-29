package com.flab.stocktradingengine.matching.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.StockSlotHasher;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.matching.worker.recovery.OwnedStockSlotRestorer;
import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * 재시작 때 담당 종목 slot 목록을 저널의 받음·넘김 항목으로 만들고(11-12절), 그 목록으로 주문 입구 녹화를 거르는 흐름.
 * 스냅샷이 없으면 빈 목록에서 시작해 저널 항목을 순서대로 적용한다.
 */
class MatchingOrderIntakeReplayJournalSlotsTest {

    private static final StockSlotHasher HASHER = new StockSlotHasher(64);
    private static final String STOCK_IN_SLOT_A = stockInSlotIndex(0);
    private static final String STOCK_IN_SLOT_B = stockInSlotIndex(1);
    private static final String STOCK_IN_SLOT_C = stockInSlotIndex(2);

    @Test
    void 스냅샷_없이_받음_항목_둘이_저널에_있으면_그_종목_slot의_녹화_주문만_남는다() {
        List<JournaledOrder> journal = List.of(
            JournaledOrder.slotAcquired(HASHER.slotFor(STOCK_IN_SLOT_A)),
            JournaledOrder.slotAcquired(HASHER.slotFor(STOCK_IN_SLOT_B)));
        List<JournaledOrder> recording = List.of(
            place(1L, STOCK_IN_SLOT_A), place(2L, STOCK_IN_SLOT_B), place(3L, STOCK_IN_SLOT_C));

        List<JournaledOrder> kept = replayInput(Set.of(), journal, recording);

        assertThat(kept).extracting(JournaledOrder::orderId).containsExactly(1L, 2L);
    }

    @Test
    void 같은_저널에_넘김_항목이_뒤따르면_넘긴_종목_slot의_주문은_제외된다() {
        List<JournaledOrder> journal = List.of(
            JournaledOrder.slotAcquired(HASHER.slotFor(STOCK_IN_SLOT_A)),
            JournaledOrder.slotAcquired(HASHER.slotFor(STOCK_IN_SLOT_B)),
            JournaledOrder.slotReleased(HASHER.slotFor(STOCK_IN_SLOT_B)));
        List<JournaledOrder> recording = List.of(
            place(1L, STOCK_IN_SLOT_A), place(2L, STOCK_IN_SLOT_B), place(3L, STOCK_IN_SLOT_C));

        List<JournaledOrder> kept = replayInput(Set.of(), journal, recording);

        assertThat(kept).extracting(JournaledOrder::orderId).containsExactly(1L);
    }

    @Test
    void 넘긴_뒤_다시_받으면_담당이_된다() {
        List<JournaledOrder> journal = List.of(
            JournaledOrder.slotAcquired(HASHER.slotFor(STOCK_IN_SLOT_A)),
            JournaledOrder.slotReleased(HASHER.slotFor(STOCK_IN_SLOT_A)),
            JournaledOrder.slotAcquired(HASHER.slotFor(STOCK_IN_SLOT_A)));

        assertThat(OwnedStockSlotRestorer.restoreFromJournal(Set.of(), journal))
            .containsExactly(HASHER.slotFor(STOCK_IN_SLOT_A));
    }

    @Test
    void 스냅샷_목록에서_시작해_저널_항목을_적용한다() {
        int snapshotSlot = HASHER.slotFor(STOCK_IN_SLOT_C);
        List<JournaledOrder> journal = List.of(
            JournaledOrder.slotAcquired(HASHER.slotFor(STOCK_IN_SLOT_A)),
            JournaledOrder.slotReleased(snapshotSlot));

        assertThat(OwnedStockSlotRestorer.restoreFromJournal(Set.of(snapshotSlot), journal))
            .containsExactly(HASHER.slotFor(STOCK_IN_SLOT_A));
    }

    @Test
    void 저널도_녹화도_비어_있으면_목록도_복구_입력도_비어_있다() {
        assertThat(OwnedStockSlotRestorer.restoreFromJournal(Set.of(), List.of())).isEmpty();
        assertThat(replayInput(Set.of(), List.of(), List.of())).isEmpty();
    }

    @Test
    void 저널의_주문_항목은_담당_목록에_영향을_주지_않는다() {
        List<JournaledOrder> journal = List.of(place(1L, STOCK_IN_SLOT_A));

        assertThat(OwnedStockSlotRestorer.restoreFromJournal(Set.of(), journal)).isEmpty();
    }

    private List<JournaledOrder> replayInput(Set<Integer> snapshotSlots, List<JournaledOrder> journal,
                                             List<JournaledOrder> recording) {
        Set<Integer> owned = OwnedStockSlotRestorer.restoreFromJournal(snapshotSlots, journal);
        return MatchingOrderIntakeConfig.filterOwnedEntries(recording, owned::contains, HASHER);
    }

    private JournaledOrder place(long orderId, String stockCode) {
        return new JournaledOrder(EventType.PLACE, orderId, 100L + orderId, stockCode, OrderSide.BUY,
            new BigDecimal("70000"), 10, Instant.parse("2026-10-29T00:00:00Z"));
    }

    /** 서로 다른 종목 slot에 속하는 종목코드를 순번대로 하나씩 고른다. */
    private static String stockInSlotIndex(int index) {
        List<Integer> seenSlots = new ArrayList<>();
        for (int code = 1; code < 10_000; code++) {
            String candidate = String.format("%06d", code);
            int slot = HASHER.slotFor(candidate);
            if (!seenSlots.contains(slot)) {
                seenSlots.add(slot);
                if (seenSlots.size() == index + 1) {
                    return candidate;
                }
            }
        }
        throw new IllegalStateException("종목 slot이 서로 다른 종목을 찾지 못했다");
    }
}
