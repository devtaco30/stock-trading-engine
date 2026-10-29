package com.flab.stocktradingengine.matching.worker.coordination;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

/** 맵 읽기가 맵 토픽의 끝(배정 시점의 end offset)까지 따라올 때까지 기다린다. */
class MapCatchUpWaiterTest {

    @Test
    void 읽은_위치가_끝_위치에_닿으면_true다() throws Exception {
        AtomicLong consumed = new AtomicLong(3);
        MapCatchUpWaiter waiter = new MapCatchUpWaiter(() -> 10L, consumed::get, 3_000L);
        Thread reader = new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                return;
            }
            consumed.set(10);
        });
        reader.start();

        assertThat(waiter.awaitCaughtUp()).isTrue();
    }

    @Test
    void 이미_닿아_있으면_바로_true다() {
        MapCatchUpWaiter waiter = new MapCatchUpWaiter(() -> 0L, () -> 0L, 3_000L);

        assertThat(waiter.awaitCaughtUp()).isTrue();
    }

    @Test
    void 시간_안에_닿지_못하면_false다() {
        MapCatchUpWaiter waiter = new MapCatchUpWaiter(() -> 10L, () -> 3L, 200L);

        assertThat(waiter.awaitCaughtUp()).isFalse();
    }

    @Test
    void 끝_위치를_알아내지_못하면_닿았다고_보지_않고_false다() {
        MapCatchUpWaiter waiter = new MapCatchUpWaiter(() -> {
            throw new IllegalStateException("브로커 응답 없음");
        }, () -> 100L, 200L);

        assertThat(waiter.awaitCaughtUp()).isFalse();
    }
}
