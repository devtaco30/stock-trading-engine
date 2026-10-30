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

    @Test
    void 따라오지_못한_판정이_연속_N번이면_읽기를_다시_시작하도록_알리고_성공하면_횟수를_처음부터_센다() {
        AtomicLong consumed = new AtomicLong(3);
        java.util.concurrent.atomic.AtomicInteger restarts = new java.util.concurrent.atomic.AtomicInteger();
        MapCatchUpWaiter waiter = new MapCatchUpWaiter(() -> 10L, consumed::get, 20L);
        waiter.onRepeatedFailure(3, restarts::incrementAndGet);

        waiter.awaitCaughtUp();
        waiter.awaitCaughtUp();
        assertThat(restarts.get()).isZero();
        waiter.awaitCaughtUp();
        assertThat(restarts.get()).as("연속 세 번째 실패에서 알린다").isEqualTo(1);

        consumed.set(10); // 따라왔다
        assertThat(waiter.awaitCaughtUp()).isTrue();
        consumed.set(3);
        waiter.awaitCaughtUp();
        waiter.awaitCaughtUp();
        assertThat(restarts.get()).as("성공이 끼어 횟수가 처음부터 다시 센다").isEqualTo(1);
        waiter.awaitCaughtUp();
        assertThat(restarts.get()).isEqualTo(2);
    }

    @Test
    void 기다리는_도중_읽기가_정상이_아니게_되면_바로_false로_돌아오고_실패로_세지_않는다() throws Exception {
        // 읽기가 재시작해 새 consumer로 처음부터 다시 읽는 동안 읽은 위치는 0부터 다시 올라온다. 이 동안의 대기는 시간을 채워도 정지의 증거가 아니다.
        java.util.concurrent.atomic.AtomicBoolean healthy = new java.util.concurrent.atomic.AtomicBoolean(true);
        java.util.concurrent.atomic.AtomicInteger restarts = new java.util.concurrent.atomic.AtomicInteger();
        MapCatchUpWaiter waiter = new MapCatchUpWaiter(() -> 10L, () -> 0L, 5_000L);
        waiter.setReaderHealthy(healthy::get);
        waiter.onRepeatedFailure(2, restarts::incrementAndGet);

        for (int i = 0; i < 5; i++) {
            Thread restartLater = new Thread(() -> {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    return;
                }
                healthy.set(false);
            });
            healthy.set(true);
            restartLater.start();
            long startedAt = System.nanoTime();

            assertThat(waiter.awaitCaughtUp()).isFalse();

            assertThat((System.nanoTime() - startedAt) / 1_000_000L).as("한도(5초)를 채우지 않고 돌아온다").isLessThan(2_000L);
        }
        assertThat(restarts).as("읽기가 이미 재시작 중이라 따라오지 못한 판정으로 세지 않는다").hasValue(0);
    }
}
