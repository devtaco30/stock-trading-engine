package com.flab.stocktradingengine.matching.worker.handover;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 응답을 기다리다 포기한 뒤에도 일정 간격으로 새 이동 번호로 다시 요청하는 A안. 시험은 재요청 간격 50ms, 포기 시간 300ms, 포기 뒤 재요청
 * 간격 400ms로 같은 규칙을 확인한다.
 */
class HandoverCoordinatorAbandonedRetryTest {

    private static final long RETRY_MILLIS = 50L;
    private static final long TIMEOUT_MILLIS = 300L;
    private static final long ABANDONED_RETRY_MILLIS = 400L;

    private static final class FakePort implements HandoverRequestPort {
        final List<Long> requestedTransferIds = new CopyOnWriteArrayList<>();
        final AtomicLong nextTransferId = new AtomicLong(100L);
        final Set<Integer> inFlight = ConcurrentHashMap.newKeySet();
        final AtomicBoolean movingEntry = new AtomicBoolean(true);

        @Override
        public Optional<Long> request(int stockSlot) {
            long transferId = nextTransferId.incrementAndGet();
            requestedTransferIds.add(transferId);
            inFlight.add(stockSlot);
            return Optional.of(transferId);
        }

        @Override
        public boolean resend(int stockSlot) {
            return true;
        }

        @Override
        public boolean inFlight(int stockSlot) {
            return inFlight.contains(stockSlot);
        }

        @Override
        public void cancel(int stockSlot) {
            inFlight.remove(stockSlot);
        }

        @Override
        public boolean hasMovingEntry(int stockSlot) {
            return movingEntry.get();
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
        coordinator = new HandoverCoordinator(port, RETRY_MILLIS, TIMEOUT_MILLIS, ABANDONED_RETRY_MILLIS);
        coordinator.start();
        return coordinator;
    }

    @Test
    void 포기한_뒤_간격이_지나면_같은_종목_slot을_새_이동_번호로_다시_요청한다() throws Exception {
        FakePort port = new FakePort();
        start(port).requestHandover(5);

        awaitUntil(() -> port.requestedTransferIds.size() >= 2);

        assertThat(port.requestedTransferIds.get(1)).as("새 요청은 새 이동 번호를 쓴다").isNotEqualTo(port.requestedTransferIds.get(0));
        assertThat(coordinator.failures()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void 포기한_뒤_간격이_지나기_전에는_다시_요청하지_않는다() throws Exception {
        FakePort port = new FakePort();
        start(port).requestHandover(5);
        awaitUntil(() -> coordinator.failures() == 1);

        Thread.sleep(150);

        assertThat(port.requestedTransferIds).hasSize(1);
    }

    @Test
    void 다시_요청한_응답이_완성되면_더_요청하지_않고_실패로도_세지_않는다() throws Exception {
        FakePort port = new FakePort();
        start(port).requestHandover(5);
        awaitUntil(() -> port.requestedTransferIds.size() >= 2);
        long failuresAtRetry = coordinator.failures();

        port.inFlight.remove(5); // 응답이 완성되어 적재가 확정된 것
        Thread.sleep(TIMEOUT_MILLIS + ABANDONED_RETRY_MILLIS + 200);

        assertThat(port.requestedTransferIds).hasSize(2);
        assertThat(coordinator.failures()).isEqualTo(failuresAtRetry);
    }

    @Test
    void 재요청_대기_중에_회수되면_더_요청하지_않는다() throws Exception {
        FakePort port = new FakePort();
        start(port).requestHandover(5);
        awaitUntil(() -> coordinator.failures() == 1);

        coordinator.cancelHandover(5);
        Thread.sleep(ABANDONED_RETRY_MILLIS + 300);

        assertThat(port.requestedTransferIds).hasSize(1);
    }

    @Test
    void 재요청_시각에_담당_대기에서_빠져_있으면_요청하지_않고_대기에서_지운다() throws Exception {
        FakePort port = new FakePort();
        start(port);
        coordinator.setStillAwaitingLoad(slot -> false);
        coordinator.requestHandover(5);
        // 첫 요청은 배정 직후라 보낸다. 포기 뒤 재요청 시각에는 담당 대기에서 빠져 있다.
        awaitUntil(() -> coordinator.failures() == 1);

        Thread.sleep(ABANDONED_RETRY_MILLIS + 300);

        assertThat(port.requestedTransferIds).hasSize(1);
    }

    @Test
    void 재요청_시각에_맵_항목이_이동_중이_아니면_요청하지_않고_대기에서_지운다() throws Exception {
        FakePort port = new FakePort();
        start(port).requestHandover(5);
        awaitUntil(() -> coordinator.failures() == 1);
        port.movingEntry.set(false);

        Thread.sleep(ABANDONED_RETRY_MILLIS + 300);

        assertThat(port.requestedTransferIds).hasSize(1);
    }

    private static void awaitUntil(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(condition.getAsBoolean()).as("3초 안에 조건이 충족되지 않음").isTrue();
    }
}
