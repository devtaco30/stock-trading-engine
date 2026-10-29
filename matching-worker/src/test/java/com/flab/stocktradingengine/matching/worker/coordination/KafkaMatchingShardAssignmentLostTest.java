package com.flab.stocktradingengine.matching.worker.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.IntFunction;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.MatchingSlotTarget;
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventRequests;

/**
 * 세션 시간을 넘겨 그룹에서 쫓겨난 뒤 돌아와 받는 「잃음」 콜백(onPartitionsLost). 그 종목 slot은 이미 다른 pod에 배정되어 그 pod가 맵에 자기
 * 주소를 올렸을 수 있으므로, 이 pod는 맵에 이동 중 표시를 쓰지 않고 로컬 담당 표시와 대기 상태만 끈다.
 */
class KafkaMatchingShardAssignmentLostTest {

    private static final String OWN = "aeron:udp?endpoint=localhost:20021";
    private static final String PREVIOUS_OWNER = "aeron:udp?endpoint=localhost:20020";

    private final MockProducer<Integer, String> mapProducer =
        new MockProducer<>(true, new IntegerSerializer(), new StringSerializer());
    private final List<Integer> handoverCancelled = new CopyOnWriteArrayList<>();
    private KafkaMatchingShardAssignment assignment;
    private ConsumerRebalanceListener listener;
    private volatile Optional<MatchingSlotTarget> mapEntry = Optional.empty();

    @BeforeEach
    void setUp() {
        IntFunction<Optional<MatchingSlotTarget>> lookup = slot -> mapEntry;
        com.flab.stocktradingengine.matching.worker.coordination.MovedSlotRequests requests =
            new com.flab.stocktradingengine.matching.worker.coordination.MovedSlotRequests() {
                @Override
                public void requestHandover(int stockSlot) {
                }

                @Override
                public void cancelHandover(int stockSlot) {
                    handoverCancelled.add(stockSlot);
                }
            };
        assignment = new KafkaMatchingShardAssignment(mock(Consumer.class), mapProducer, mock(Admin.class), 64, OWN,
            "assignment-topic", "map-topic", new SlotEventRequests(), lookup, requests, () -> true, Runnable::run);
        listener = assignment.rebalanceListener();
    }

    @AfterEach
    void tearDown() {
        assignment.close();
    }

    @Test
    void 담당_중인_종목_slot을_잃으면_맵에_쓰지_않고_담당_표시만_끈다() {
        listener.onPartitionsAssigned(partitions(5));
        assignment.confirmAcquired(5);
        assertThat(assignment.test(5)).isTrue();
        mapProducer.clear();

        listener.onPartitionsLost(partitions(5));

        assertThat(assignment.test(5)).isFalse();
        assertThat(mapProducer.history()).as("이미 다른 pod가 올린 값을 덮지 않도록 맵에 쓰지 않는다").isEmpty();
    }

    @Test
    void 호가창_적재를_기다리던_종목_slot을_잃으면_대기를_지우고_요청을_취소하며_맵에_쓰지_않는다() {
        mapEntry = Optional.of(new MatchingSlotTarget(PREVIOUS_OWNER, true));
        listener.onPartitionsAssigned(partitions(5));
        assertThat(assignment.slotsAwaitingLoad()).containsExactly(5);
        mapProducer.clear();

        listener.onPartitionsLost(partitions(5));

        assertThat(assignment.slotsAwaitingLoad()).isEmpty();
        assertThat(assignment.stuckSlotCount()).isZero();
        assertThat(handoverCancelled).containsExactly(5);
        assertThat(mapProducer.history()).isEmpty();
    }

    private static List<TopicPartition> partitions(int... slots) {
        return java.util.Arrays.stream(slots).mapToObj(slot -> new TopicPartition("assignment-topic", slot)).toList();
    }
}
