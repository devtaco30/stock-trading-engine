package com.flab.stocktradingengine.matching.disruptor.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

/**
 * 받음 항목의 Archive 기록 확인이 막혔을 때의 경고(보정 4). 기록 위치를 읽을 수 없거나(음수) 대기 항목이 오래 남으면 경고를 남기되,
 * 폴링 반복마다 쏟아내지 않고 일정 간격으로 한 번씩만 남긴다.
 */
class AcquireConfirmationsWarningTest {

    private static final long SECOND = 1_000_000_000L;

    private final AtomicLong clock = new AtomicLong(1_000 * SECOND);
    private final AtomicLong recorded = new AtomicLong(-1L);
    private final AcquireConfirmations confirmations = new AcquireConfirmations(recorded::get, clock::get);

    @Test
    void 기록_위치를_읽을_수_없으면_경고를_한_번_남기고_간격이_지나기_전에는_다시_남기지_않는다() {
        confirmations.awaitRecorded(5, 100L);

        confirmations.confirmRecorded(SlotEventListener.NO_OP);
        assertEquals(1, confirmations.warningsLogged());
        for (int i = 0; i < 1_000; i++) {
            confirmations.confirmRecorded(SlotEventListener.NO_OP);
        }
        assertEquals(1, confirmations.warningsLogged(), "폴링 반복마다 경고를 남기면 안 된다");

        clock.addAndGet(11 * SECOND);
        confirmations.confirmRecorded(SlotEventListener.NO_OP);
        assertEquals(2, confirmations.warningsLogged());
    }

    @Test
    void 대기_항목이_한도보다_오래_남으면_경고한다() {
        recorded.set(50L); // 읽을 수는 있지만 항목 끝 위치(100)에 닿지 않는다
        confirmations.awaitRecorded(5, 100L);

        confirmations.confirmRecorded(SlotEventListener.NO_OP);
        assertEquals(0, confirmations.warningsLogged(), "한도 안에서는 경고하지 않는다");

        clock.addAndGet(6 * SECOND);
        confirmations.confirmRecorded(SlotEventListener.NO_OP);
        assertEquals(1, confirmations.warningsLogged());
        clock.addAndGet(2 * SECOND);
        confirmations.confirmRecorded(SlotEventListener.NO_OP);
        assertEquals(1, confirmations.warningsLogged(), "간격 안에서는 다시 남기지 않는다");
    }

    @Test
    void 기록_위치가_닿아_통지하면_더_경고하지_않는다() {
        confirmations.awaitRecorded(5, 100L);
        clock.addAndGet(6 * SECOND);
        confirmations.confirmRecorded(SlotEventListener.NO_OP);
        assertTrue(confirmations.warningsLogged() >= 1);
        long before = confirmations.warningsLogged();

        recorded.set(100L);
        assertEquals(1, confirmations.confirmRecorded(SlotEventListener.NO_OP));
        clock.addAndGet(60 * SECOND);
        confirmations.confirmRecorded(SlotEventListener.NO_OP);

        assertEquals(before, confirmations.warningsLogged());
    }
}
