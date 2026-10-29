package com.flab.stocktradingengine.matching.worker.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventRequests;

/**
 * 이동으로 받는 종목 slot(맵 항목이 {@code X주소;moving})의 배정 콜백(U15b). Z는 자기 주소를 맵에 올리지 않고(맵은 호가창 적재가
 * 끝날 때까지 이동 중으로 남는다), 이전 담당에게 호가창을 요청하며, 호가창 적재와 스냅샷 확정이 끝나기 전에는 담당 표시를
 * 켜지 않는다. 항목이 없는 첫 배정은 예전처럼 자기 주소를 올리고 받음 요청을 남긴다.
 */
class KafkaMatchingShardAssignmentMovedReceiveTest {

    private static final String OWN = "aeron:udp?endpoint=localhost:20021";
    private static final String PREVIOUS_OWNER = "aeron:udp?endpoint=localhost:20020";

    private final SlotEventRequests requests = new SlotEventRequests();
    private final MockProducer<Integer, String> mapProducer =
        new MockProducer<>(true, new IntegerSerializer(), new StringSerializer());
    private final List<Integer> handoverRequested = new ArrayList<>();
    private Optional<MatchingSlotTarget> mapEntry = Optional.empty();
    private MatchingEngine engine;
    private KafkaMatchingShardAssignment assignment;
    private ConsumerRebalanceListener listener;

    @BeforeEach
    void setUp() {
        engine = new MatchingEngine(1024, (stockCode, fill) -> { });
        engine.start();
        IntFunction<Optional<MatchingSlotTarget>> mapLookup = slot -> mapEntry;
        assignment = new KafkaMatchingShardAssignment(mock(Consumer.class), mapProducer, mock(Admin.class), 64, OWN,
            "assignment-topic", "map-topic", requests, mapLookup, handoverRequested::add);
        listener = assignment.rebalanceListener();
    }

    @AfterEach
    void tearDown() {
        engine.shutdown();
    }

    @Test
    void 맵_항목이_이동_중이면_자기_주소를_맵에_올리지_않고_호가창을_요청한다() {
        mapEntry = Optional.of(new MatchingSlotTarget(PREVIOUS_OWNER, true));

        listener.onPartitionsAssigned(partitions(5));

        assertThat(mapProducer.history()).isEmpty();
        assertThat(handoverRequested).containsExactly(5);
    }

    @Test
    void 이동으로_받는_종목_slot은_받음_요청을_남기지_않고_담당도_아니다() {
        mapEntry = Optional.of(new MatchingSlotTarget(PREVIOUS_OWNER, true));

        listener.onPartitionsAssigned(partitions(5));

        assertThat(requests.drainTo(engine)).isZero();
        assertThat(assignment.test(5)).isFalse();
    }

    @Test
    void 적재와_스냅샷_확정이_끝났다고_알리면_그때_받음_요청을_남기고_저널_기록_확인이_와야_담당이_된다() throws Exception {
        mapEntry = Optional.of(new MatchingSlotTarget(PREVIOUS_OWNER, true));
        listener.onPartitionsAssigned(partitions(5));

        assignment.acquireAfterLoad(5);

        assertThat(assignment.test(5)).isFalse();
        assertThat(requests.drainTo(engine)).isEqualTo(1);
        awaitJournalSize(1);
        assertThat(engine.journal().entries().get(0).type()).isEqualTo(EventType.SLOT_ACQUIRED);
        assignment.confirmAcquired(5);
        assertThat(assignment.test(5)).isTrue();
    }

    @Test
    void 맵에_항목이_없는_첫_배정은_자기_주소를_올리고_받음_요청을_남긴다() {
        mapEntry = Optional.empty();

        listener.onPartitionsAssigned(partitions(5));

        assertThat(mapProducer.history()).hasSize(1);
        assertThat(mapProducer.history().get(0).value()).isEqualTo(OWN);
        assertThat(handoverRequested).isEmpty();
        assertThat(requests.drainTo(engine)).isEqualTo(1);
    }

    @Test
    void 맵의_이동_중_항목이_자기_주소이면_이동이_아니라_재시작으로_보고_평상시_값으로_되돌린다() {
        mapEntry = Optional.of(new MatchingSlotTarget(OWN, true));

        listener.onPartitionsAssigned(partitions(5));

        assertThat(handoverRequested).isEmpty();
        assertThat(mapProducer.history()).hasSize(1);
        assertThat(mapProducer.history().get(0).value()).isEqualTo(OWN);
        assertThat(requests.drainTo(engine)).isEqualTo(1);
        assertThat(assignment.test(5)).isFalse();
        assignment.confirmAcquired(5);
        assertThat(assignment.test(5)).isTrue();
    }

    @Test
    void 호가창을_기다리는_종목_slot이_회수되면_늦게_온_적재_완료는_받음_요청을_남기지_않는다() {
        mapEntry = Optional.of(new MatchingSlotTarget(PREVIOUS_OWNER, true));
        listener.onPartitionsAssigned(partitions(5));
        listener.onPartitionsRevoked(partitions(5));
        requests.drainTo(engine);

        assignment.acquireAfterLoad(5);

        assertThat(requests.drainTo(engine)).isZero();
    }

    private static List<TopicPartition> partitions(int... slots) {
        return java.util.Arrays.stream(slots).mapToObj(slot -> new TopicPartition("assignment-topic", slot)).toList();
    }

    private void awaitJournalSize(int size) throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (engine.journal().entries().size() < size && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(engine.journal().entries().size()).isGreaterThanOrEqualTo(size);
    }
}
