package com.flab.stocktradingengine.matching.worker.handover;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 응답 대기 한도와 같은 이동 번호로 하는 재요청(U15c). Jack이 정한 값은 재요청 간격 1초, 포기 시간 5초다. 시험은 간격 50ms,
 * 포기 시간 400ms로 같은 규칙을 확인한다.
 */
class HandoverCoordinatorRetryTest {

    private static final long RETRY_MILLIS = 50L;
    private static final long TIMEOUT_MILLIS = 400L;

    /** 요청 전송 지점을 흉내 내는 가짜. 응답이 완성되어 끝났는지는 test가 정한다. */
    private static final class FakePort implements HandoverRequestPort {
        final List<String> calls = new CopyOnWriteArrayList<>();
        final AtomicInteger resendCount = new AtomicInteger();
        final AtomicBoolean mapHasMovingEntry = new AtomicBoolean(true);
        // true인 동안 request가 예외를 던진다(이전 담당 주소가 전송 풀에 없을 때 HandoverRequester.requireInPool이 던지는 경우).
        final AtomicBoolean requestThrows = new AtomicBoolean(false);
        final Set<Integer> inFlight = ConcurrentHashMap.newKeySet();
        final List<Integer> cancelled = new CopyOnWriteArrayList<>();
        final long transferId = 77L;

        @Override
        public Optional<Long> request(int stockSlot) {
            calls.add("request:" + stockSlot);
            if (requestThrows.get()) {
                throw new IllegalStateException("이전 담당 주소가 풀에 없음");
            }
            if (!mapHasMovingEntry.get()) {
                return Optional.empty();
            }
            inFlight.add(stockSlot);
            return Optional.of(transferId);
        }

        @Override
        public boolean resend(int stockSlot) {
            calls.add("resend:" + stockSlot);
            resendCount.incrementAndGet();
            return true;
        }

        @Override
        public boolean inFlight(int stockSlot) {
            return inFlight.contains(stockSlot);
        }

        @Override
        public void cancel(int stockSlot) {
            cancelled.add(stockSlot);
            inFlight.remove(stockSlot);
        }
    }

    private HandoverCoordinator coordinator;

    @AfterEach
    void tearDown() {
        if (coordinator != null) {
            coordinator.close();
        }
    }

    private HandoverCoordinator start(FakePort port) {
        coordinator = new HandoverCoordinator(port, RETRY_MILLIS, TIMEOUT_MILLIS);
        coordinator.start();
        return coordinator;
    }

    @Test
    void 응답이_오지_않으면_같은_요청을_간격을_두고_다시_보내고_포기_시간에_실패로_끝난다() throws Exception {
        FakePort port = new FakePort();
        start(port).requestHandover(5);

        awaitUntil(() -> coordinator.failures() == 1);

        int resends = port.resendCount.get();
        assertThat(resends).as("포기 시간(400ms) 안에 간격(50ms)마다 재요청: 너무 적으면 재요청이 안 나간 것이고 너무 많으면 간격이 없는 것이다")
            .isBetween(3, 9);
        assertThat(port.calls.get(0)).isEqualTo("request:5");
        assertThat(port.cancelled).as("실패로 끝나면 진행 중 요청을 취소해 늦게 온 응답이 버려진다").containsExactly(5);
        Thread.sleep(200);
        assertThat(port.resendCount.get()).as("실패 뒤에는 더 보내지 않는다").isEqualTo(resends);
    }

    @Test
    void 응답이_완성되어_진행_중이_아니게_되면_재요청을_멈춘다() throws Exception {
        FakePort port = new FakePort();
        start(port).requestHandover(5);
        awaitUntil(() -> port.resendCount.get() >= 1);

        port.inFlight.remove(5); // 응답이 완성되어 적재 요청이 나간 것
        Thread.sleep(150);
        int resends = port.resendCount.get();
        Thread.sleep(300);

        assertThat(port.resendCount.get()).isEqualTo(resends);
        assertThat(coordinator.failures()).isZero();
        assertThat(port.cancelled).isEmpty();
    }

    @Test
    void 맵에_이동_중_항목이_아직_없어_요청을_못_보내면_간격을_두고_다시_시도하다가_나타나면_보낸다() throws Exception {
        FakePort port = new FakePort();
        port.mapHasMovingEntry.set(false);
        start(port).requestHandover(5);
        Thread.sleep(150);
        assertThat(port.calls).as("항목이 없는 동안 요청 시도만 반복된다").allMatch(call -> call.equals("request:5"));
        assertThat(port.calls.size()).isBetween(2, 5);

        port.mapHasMovingEntry.set(true);
        awaitUntil(() -> port.inFlight.contains(5));

        assertThat(coordinator.failures()).isZero();
    }

    @Test
    void 끝내_이동_중_항목이_나타나지_않으면_포기_시간에_실패로_끝난다() throws Exception {
        FakePort port = new FakePort();
        port.mapHasMovingEntry.set(false);
        start(port).requestHandover(5);

        awaitUntil(() -> coordinator.failures() == 1);

        assertThat(port.resendCount.get()).isZero();
    }

    @Test
    void 첫_요청이_예외를_던져도_추적을_이어가_간격을_두고_다시_시도해_보낸다() throws Exception {
        FakePort port = new FakePort();
        port.requestThrows.set(true);
        start(port).requestHandover(5);
        Thread.sleep(150);
        assertThat(port.calls.size()).as("예외가 나도 요청 시도가 간격을 두고 반복된다").isBetween(2, 5);

        port.requestThrows.set(false);
        awaitUntil(() -> port.inFlight.contains(5));

        assertThat(coordinator.failures()).isZero();
    }

    @Test
    void 요청이_계속_예외를_던지면_포기_시간에_실패로_세고_취소한다() throws Exception {
        FakePort port = new FakePort();
        port.requestThrows.set(true);
        start(port).requestHandover(5);

        awaitUntil(() -> coordinator.failures() == 1);

        assertThat(port.cancelled).containsExactly(5);
    }

    @Test
    void 회수로_취소하면_추적을_멈추고_실패로_세지_않는다() throws Exception {
        FakePort port = new FakePort();
        start(port).requestHandover(5);
        awaitUntil(() -> port.resendCount.get() >= 1);

        coordinator.cancelHandover(5);
        Thread.sleep(500);

        assertThat(coordinator.failures()).isZero();
        assertThat(port.cancelled).contains(5);
    }

    private static void awaitUntil(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(condition.getAsBoolean()).as("3초 안에 조건이 충족되지 않음").isTrue();
    }
}
