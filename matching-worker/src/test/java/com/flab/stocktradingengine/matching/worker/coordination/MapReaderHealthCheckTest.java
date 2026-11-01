package com.flab.stocktradingengine.matching.worker.coordination;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

/**
 * 판정이 없어도 맵 읽기가 정지했는지 본다. 주기마다 맵 토픽의 끝 위치와 읽은 위치를 비교해, 끝에 못 닿았는데 읽은 위치가 앞 점검 때와
 * 같은 채로 연속 한도만큼 이어지면 읽기 재시작을 요청한다.
 */
class MapReaderHealthCheckTest {

    private final AtomicLong end = new AtomicLong(10);
    private final AtomicLong consumed = new AtomicLong(10);
    private final AtomicBoolean healthy = new AtomicBoolean(true);
    private final AtomicInteger restarts = new AtomicInteger();
    private final MapReaderHealthCheck check = new MapReaderHealthCheck(
        end::get, consumed::get, healthy::get, 3, restarts::incrementAndGet);

    @Test
    void 읽은_위치가_끝_위치에_닿아_있으면_재시작을_요청하지_않는다() {
        for (int i = 0; i < 10; i++) {
            check.checkOnce();
        }

        assertThat(restarts).hasValue(0);
    }

    @Test
    void 끝_위치에_못_닿은_채_읽은_위치가_멈춰_연속_한도만큼_이어지면_재시작을_한_번_요청한다() {
        consumed.set(4);

        check.checkOnce(); // 처음 본 위치라 멈췄는지 알 수 없다
        check.checkOnce(); // 같은 위치 1번째
        check.checkOnce(); // 2번째
        assertThat(restarts).hasValue(0);
        check.checkOnce(); // 3번째

        assertThat(restarts).hasValue(1);
    }

    @Test
    void 요청한_뒤에는_한도만큼_다시_멈춰_있어야_또_요청한다() {
        consumed.set(4);
        for (int i = 0; i < 4; i++) {
            check.checkOnce();
        }
        assertThat(restarts).hasValue(1);

        check.checkOnce(); // 요청 뒤 처음 본 위치
        check.checkOnce();
        check.checkOnce();
        assertThat(restarts).hasValue(1);
        check.checkOnce();

        assertThat(restarts).hasValue(2);
    }

    @Test
    void 읽은_위치가_조금이라도_움직이면_멈춘_횟수를_처음부터_센다() {
        consumed.set(4);
        check.checkOnce();
        check.checkOnce();
        check.checkOnce();

        consumed.set(5);
        check.checkOnce();
        check.checkOnce();
        check.checkOnce();

        assertThat(restarts).hasValue(0);
        check.checkOnce();
        assertThat(restarts).hasValue(1);
    }

    @Test
    void 따라잡으면_멈춘_횟수를_처음부터_센다() {
        consumed.set(4);
        check.checkOnce();
        check.checkOnce();
        check.checkOnce();

        consumed.set(10);
        check.checkOnce();
        consumed.set(4);
        check.checkOnce();
        check.checkOnce();
        check.checkOnce();

        assertThat(restarts).hasValue(0);
    }

    @Test
    void 읽기가_이미_정상이_아니면_세지_않고_횟수도_버린다() {
        consumed.set(4);
        check.checkOnce();
        check.checkOnce();
        check.checkOnce();

        healthy.set(false);
        for (int i = 0; i < 10; i++) {
            check.checkOnce();
        }
        healthy.set(true);
        check.checkOnce();
        check.checkOnce();
        check.checkOnce();

        assertThat(restarts).hasValue(0);
    }

    @Test
    void 끝_위치를_알아내지_못하면_세지_않고_예외도_내보내지_않는다() {
        MapReaderHealthCheck unreachable = new MapReaderHealthCheck(
            () -> {
                throw new IllegalStateException("브로커 응답 없음");
            },
            consumed::get, healthy::get, 3, restarts::incrementAndGet);
        consumed.set(4);

        for (int i = 0; i < 10; i++) {
            unreachable.checkOnce();
        }

        assertThat(restarts).hasValue(0);
    }

    @Test
    void 시작하면_주기마다_점검하고_닫으면_멈춘다() throws Exception {
        consumed.set(4);
        MapReaderHealthCheck periodic = new MapReaderHealthCheck(
            end::get, consumed::get, healthy::get, 2, restarts::incrementAndGet);

        periodic.start(20L);
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (restarts.get() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        periodic.close();
        int afterClose = restarts.get();
        Thread.sleep(200);

        assertThat(afterClose).as("3초 안에 재시작 요청이 와야 한다").isGreaterThan(0);
        assertThat(restarts.get()).as("닫은 뒤에는 점검하지 않는다").isEqualTo(afterClose);
    }
}
