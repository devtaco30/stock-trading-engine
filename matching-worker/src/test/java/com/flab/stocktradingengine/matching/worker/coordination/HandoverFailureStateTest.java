package com.flab.stocktradingengine.matching.worker.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.IntFunction;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.MatchingSlotTarget;
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventRequests;
import com.flab.stocktradingengine.matching.worker.handover.HandoverCoordinator;
import com.flab.stocktradingengine.matching.worker.handover.HandoverRequestPort;

/**
 * 응답을 끝내 받지 못해 이동이 실패로 끝난 종목 slot의 이후 상태(U15c). 담당은 꺼진 채이고, 맵은 쓰지 않아 이전 담당의 이동 중 값
 * 그대로이며, 빈 호가창으로 자동 시작하지 않는다. 포기 뒤 다시 요청하는
 * 간격이 지나기 전까지 이 상태가 유지되고, 간격이 지나면 새 이동 번호로 다시 요청한다(A안, 아래 둘째 시험).
 */
class HandoverFailureStateTest {

    private static final String OWN = "aeron:udp?endpoint=localhost:20021";
    private static final String PREVIOUS_OWNER = "aeron:udp?endpoint=localhost:20020";

    private HandoverCoordinator coordinator;

    @AfterEach
    void tearDown() {
        if (coordinator != null) {
            coordinator.close();
        }
    }

    @Test
    void 이동이_포기_시간을_넘겨_실패로_끝나도_담당은_꺼진_채이고_맵을_쓰지_않으며_요청은_취소된다() throws Exception {
        List<Integer> cancelled = new CopyOnWriteArrayList<>();
        Set<Integer> inFlight = ConcurrentHashMap.newKeySet();
        HandoverRequestPort neverAnswered = new HandoverRequestPort() {
            @Override
            public Optional<Long> request(int stockSlot) {
                inFlight.add(stockSlot);
                return Optional.of(5L);
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
                cancelled.add(stockSlot);
                inFlight.remove(stockSlot);
            }
        };
        coordinator = new HandoverCoordinator(neverAnswered, 30L, 200L);
        coordinator.start();
        MockProducer<Integer, String> mapProducer = new MockProducer<>(true, new IntegerSerializer(), new StringSerializer());
        SlotEventRequests requests = new SlotEventRequests();
        IntFunction<Optional<MatchingSlotTarget>> map = slot -> Optional.of(new MatchingSlotTarget(PREVIOUS_OWNER, true));
        KafkaMatchingShardAssignment assignment = new KafkaMatchingShardAssignment(mock(Consumer.class), mapProducer,
            mock(Admin.class), 64, OWN, "assignment-topic", "map-topic", requests, map, coordinator);

        assignment.rebalanceListener().onPartitionsAssigned(List.of(new TopicPartition("assignment-topic", 5)));
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (coordinator.failures() < 1 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        Thread.sleep(300);

        assertThat(coordinator.failures()).isEqualTo(1);
        assertThat(cancelled).as("실패로 끝나면 진행 중 요청이 취소되어 늦은 응답이 버려진다").contains(5);
        assertThat(assignment.test(5)).as("담당 표시는 꺼진 채다(빈 호가창으로 자동 시작하지 않는다)").isFalse();
        assertThat(mapProducer.history()).as("맵은 이전 담당의 이동 중 값 그대로다").isEmpty();
        assertThat(assignment.slotsAwaitingLoad()).as("다시 요청하는 간격이 지나기 전까지 호가창을 기다리는 상태로 남는다").containsExactly(5);

        // 다음 회수 때는 맵을 덮어쓰지 않고 정리된다.
        assignment.rebalanceListener().onPartitionsRevoked(List.of(new TopicPartition("assignment-topic", 5)));
        assertThat(mapProducer.history()).isEmpty();
        assertThat(assignment.slotsAwaitingLoad()).isEmpty();
    }

    @Test
    void 포기_뒤_간격이_지나면_새_이동_번호로_다시_요청하고_그_사이에도_담당은_꺼진_채이며_맵을_쓰지_않는다() throws Exception {
        List<Long> requested = new CopyOnWriteArrayList<>();
        Set<Integer> inFlight = ConcurrentHashMap.newKeySet();
        java.util.concurrent.atomic.AtomicLong ids = new java.util.concurrent.atomic.AtomicLong(10L);
        HandoverRequestPort neverAnswered = new HandoverRequestPort() {
            @Override
            public Optional<Long> request(int stockSlot) {
                inFlight.add(stockSlot);
                long id = ids.incrementAndGet();
                requested.add(id);
                return Optional.of(id);
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
        };
        coordinator = new HandoverCoordinator(neverAnswered, 30L, 150L, 200L);
        MockProducer<Integer, String> mapProducer = new MockProducer<>(true, new IntegerSerializer(), new StringSerializer());
        IntFunction<Optional<MatchingSlotTarget>> map = slot -> Optional.of(new MatchingSlotTarget(PREVIOUS_OWNER, true));
        KafkaMatchingShardAssignment assignment = new KafkaMatchingShardAssignment(mock(Consumer.class), mapProducer,
            mock(Admin.class), 64, OWN, "assignment-topic", "map-topic", new SlotEventRequests(), map, coordinator);
        coordinator.setStillAwaitingLoad(assignment::isAwaitingLoad);
        coordinator.start();

        assignment.rebalanceListener().onPartitionsAssigned(List.of(new TopicPartition("assignment-topic", 5)));
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (requested.size() < 2 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }

        assertThat(requested).hasSizeGreaterThanOrEqualTo(2);
        assertThat(requested.get(1)).as("다시 요청은 새 이동 번호를 쓴다").isNotEqualTo(requested.get(0));
        assertThat(assignment.test(5)).isFalse();
        assertThat(mapProducer.history()).isEmpty();
        assertThat(assignment.slotsAwaitingLoad()).containsExactly(5);

        assignment.rebalanceListener().onPartitionsRevoked(List.of(new TopicPartition("assignment-topic", 5)));
        int requestsAtRevoke = requested.size();
        Thread.sleep(500);
        assertThat(requested.size()).as("회수된 뒤에는 다시 요청하지 않는다").isEqualTo(requestsAtRevoke);
        assertThat(mapProducer.history()).isEmpty();
    }
}
