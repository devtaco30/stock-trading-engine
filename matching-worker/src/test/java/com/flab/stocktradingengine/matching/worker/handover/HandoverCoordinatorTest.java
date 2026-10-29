package com.flab.stocktradingengine.matching.worker.handover;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

/** 배정 콜백 thread가 넣은 종목 slot을 별도 thread가 하나씩 이전 담당에게 요청한다(U15b). Kafka 콜백이 요청 전송을 기다리지 않게 한다. */
class HandoverCoordinatorTest {

    @Test
    void 넣은_순서대로_다른_thread가_요청한다() throws Exception {
        List<Integer> requested = new CopyOnWriteArrayList<>();
        List<String> threadNames = new CopyOnWriteArrayList<>();
        HandoverCoordinator coordinator = new HandoverCoordinator(slot -> {
            requested.add(slot);
            threadNames.add(Thread.currentThread().getName());
            return Optional.of(1L);
        });
        coordinator.start();
        try {
            coordinator.requestHandover(3);
            coordinator.requestHandover(9);
            coordinator.requestHandover(4);

            awaitSize(requested, 3);

            assertThat(requested).containsExactly(3, 9, 4);
            assertThat(threadNames).doesNotContain(Thread.currentThread().getName());
        } finally {
            coordinator.close();
        }
    }

    @Test
    void 요청_중_예외가_나도_다음_종목_slot을_계속_요청한다() throws Exception {
        List<Integer> requested = new CopyOnWriteArrayList<>();
        HandoverCoordinator coordinator = new HandoverCoordinator(slot -> {
            requested.add(slot);
            if (slot == 3) {
                throw new IllegalStateException("풀에 없는 주소");
            }
            return Optional.of(1L);
        });
        coordinator.start();
        try {
            coordinator.requestHandover(3);
            coordinator.requestHandover(4);

            awaitSize(requested, 2);

            assertThat(requested).containsExactly(3, 4);
        } finally {
            coordinator.close();
        }
    }

    private static void awaitSize(List<?> list, int size) throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (list.size() < size && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(list).hasSizeGreaterThanOrEqualTo(size);
    }
}
