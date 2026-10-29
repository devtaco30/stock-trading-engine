package com.flab.stocktradingengine.matching.worker.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;

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

import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventRequests;

/**
 * 종목 slot 담당(ownedSlots)이 켜지는 시점. Kafka가 종목 slot을 배정해도 받음 이벤트가 저널에 기록되었다는 확인
 * ({@code confirmAcquired})이 오기 전에는 켜지지 않는다. 회수는 즉시 끈다. Kafka 브로커 없이 배정·회수 콜백을
 * 직접 불러 확인한다.
 */
class KafkaMatchingShardAssignmentJournalConfirmTest {

    private static final String TOPIC = "matching-shard-assignment";
    private static final int STOCK_SLOT_COUNT = 64;

    private final SlotEventRequests requests = new SlotEventRequests();
    private final MockProducer<Integer, String> mapProducer =
        new MockProducer<>(true, new IntegerSerializer(), new StringSerializer());
    private MatchingEngine engine;
    private ConsumerRebalanceListener listener;
    private KafkaMatchingShardAssignment assignment;

    @BeforeEach
    void setUp() {
        engine = new MatchingEngine(1024, (stockCode, fill) -> { });
        engine.start();
        assignment = new KafkaMatchingShardAssignment(mock(Consumer.class), mapProducer, mock(Admin.class),
            STOCK_SLOT_COUNT, "order-endpoint", "assignment-topic", "map-topic", requests);
        listener = assignment.rebalanceListener();
    }

    @AfterEach
    void tearDown() {
        engine.shutdown();
    }

    @Test
    void 배정_콜백이_와도_저널_기록_확인_전에는_담당이_아니다() {
        listener.onPartitionsAssigned(partitions(3, 5));

        assertThat(assignment.test(3)).isFalse();
        assertThat(assignment.test(5)).isFalse();
        assertThat(assignment.currentSlotsSnapshot()).isEmpty();
    }

    @Test
    void 배정_콜백은_받음_요청을_종목_slot마다_하나씩_남긴다() throws Exception {
        listener.onPartitionsAssigned(partitions(3, 5));

        assertThat(requests.drainTo(engine)).isEqualTo(2);
        awaitJournalSize(2);
        List<JournaledOrder> entries = engine.journal().entries();
        assertThat(entries).extracting(JournaledOrder::type).containsOnly(EventType.SLOT_ACQUIRED);
        assertThat(entries).extracting(JournaledOrder::stockSlot).containsExactlyInAnyOrder(3, 5);
    }

    @Test
    void 저널_기록_확인이_오면_그_종목_slot만_담당이_된다() {
        listener.onPartitionsAssigned(partitions(3, 5));

        assignment.confirmAcquired(3);

        assertThat(assignment.test(3)).isTrue();
        assertThat(assignment.test(5)).isFalse();
    }

    @Test
    void 회수_콜백은_담당을_즉시_끄고_넘김_요청을_남긴다() throws Exception {
        listener.onPartitionsAssigned(partitions(3));
        assignment.confirmAcquired(3);
        requests.drainTo(engine);

        listener.onPartitionsRevoked(partitions(3));

        assertThat(assignment.test(3)).isFalse();
        assertThat(requests.drainTo(engine)).isEqualTo(1);
        awaitJournalSize(2);
        assertThat(engine.journal().entries().get(1).type()).isEqualTo(EventType.SLOT_RELEASED);
    }

    @Test
    void 기록_확인_전에_회수되면_늦게_온_확인이_담당을_켜지_않는다() {
        listener.onPartitionsAssigned(partitions(3));
        listener.onPartitionsRevoked(partitions(3));

        assignment.confirmAcquired(3);

        assertThat(assignment.test(3)).isFalse();
    }

    @Test
    void 요청_통로를_주지_않는_기존_구성은_배정_즉시_담당이_된다() {
        KafkaMatchingShardAssignment legacy = new KafkaMatchingShardAssignment(mock(Consumer.class), mapProducer,
            mock(Admin.class), STOCK_SLOT_COUNT, "order-endpoint");

        legacy.rebalanceListener().onPartitionsAssigned(partitions(3));

        assertThat(legacy.test(3)).isTrue();
    }

    private static List<TopicPartition> partitions(int... slots) {
        return java.util.Arrays.stream(slots).mapToObj(slot -> new TopicPartition(TOPIC, slot)).toList();
    }

    private void awaitJournalSize(int size) throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (engine.journal().entries().size() < size && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(engine.journal().entries().size()).isGreaterThanOrEqualTo(size);
    }
}
