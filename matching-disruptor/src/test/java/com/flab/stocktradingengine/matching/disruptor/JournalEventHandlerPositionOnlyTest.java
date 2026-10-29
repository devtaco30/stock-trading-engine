package com.flab.stocktradingengine.matching.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.matching.disruptor.engine.OrderEvent;
import com.flab.stocktradingengine.matching.disruptor.handler.JournalEventHandler;
import com.flab.stocktradingengine.matching.disruptor.journal.Journal;

/**
 * 위치만 담은 이벤트가 {@link JournalEventHandler}를 지날 때 슬롯의 저널 위치(journaledPosition)를
 * 채우는지 슬롯 값으로 직접 확인한다. 매칭 핸들러는 이 값을 lastJournaledPosition으로 읽는다.
 */
class JournalEventHandlerPositionOnlyTest {

    @Test
    void 위치만_담은_이벤트의_슬롯_저널_위치는_저널의_현재_위치로_채워지고_저널에는_기록되지_않는다() {
        FixedPositionJournal journal = new FixedPositionJournal(4_096L);
        JournalEventHandler handler = new JournalEventHandler(journal);
        OrderEvent slot = new OrderEvent();
        slot.setPositionOnly(500L, 7);

        handler.onEvent(slot, 0L, true);

        assertEquals(4_096L, slot.getJournaledPosition());
        assertTrue(journal.entries().isEmpty(), "위치만 담은 이벤트는 저널에 기록하지 않는다");
    }

    /** position()이 고정값인 저널. InMemoryJournal은 항상 0을 돌려줘 슬롯에 값이 실렸는지 구분할 수 없다. */
    private static final class FixedPositionJournal implements Journal {
        private final long position;
        private final List<JournaledOrder> appended = new ArrayList<>();

        private FixedPositionJournal(long position) {
            this.position = position;
        }

        @Override
        public void append(JournaledOrder order) {
            appended.add(order);
        }

        @Override
        public List<JournaledOrder> entries() {
            return List.copyOf(appended);
        }

        @Override
        public long position() {
            return position;
        }
    }
}
