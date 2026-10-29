package com.flab.stocktradingengine.matching.worker.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

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
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventRequests;

/**
 * 이동으로 받은 종목 slot의 맵 전환(U16a). 담당 표시가 켜진 직후에야 맵에 이 shard의 평상시 주소를 올린다. 그 전에는 맵이
 * 이전 담당의 이동 중 값으로 남아 계좌 shard가 주문을 보관한다. 쓰기는 브로커 응답을 기다리지 않는 비동기이고 실패하면 다시 보낸다.
 */
class KafkaMatchingShardAssignmentMapSwitchTest {

    private static final String OWN = "aeron:udp?endpoint=localhost:20021";
    private static final String PREVIOUS_OWNER = "aeron:udp?endpoint=localhost:20020";

    private final MockProducer<Integer, String> mapProducer =
        new MockProducer<>(false, new IntegerSerializer(), new StringSerializer());
    private KafkaMatchingShardAssignment assignment;
    private ConsumerRebalanceListener listener;

    @BeforeEach
    void setUp() {
        IntFunction<Optional<MatchingSlotTarget>> mapLookup = slot -> Optional.of(new MatchingSlotTarget(PREVIOUS_OWNER, true));
        assignment = new KafkaMatchingShardAssignment(mock(Consumer.class), mapProducer, mock(Admin.class), 64, OWN,
            "assignment-topic", "map-topic", new SlotEventRequests(), mapLookup, slot -> { });
        assignment.setMapWriteRetryMillis(30L);
        listener = assignment.rebalanceListener();
    }

    @AfterEach
    void tearDown() {
        assignment.close();
    }

    @Test
    void 적재_확정과_받음_요청만으로는_맵에_자기_주소를_올리지_않는다() {
        listener.onPartitionsAssigned(partitions(5));

        assignment.acquireAfterLoad(5);

        assertThat(mapProducer.history()).isEmpty();
    }

    @Test
    void 담당_표시가_켜진_직후_맵에_자기_주소의_평상시_값을_한_번_올린다() throws Exception {
        listener.onPartitionsAssigned(partitions(5));
        assignment.acquireAfterLoad(5);

        assignment.confirmAcquired(5);

        assertThat(assignment.test(5)).isTrue();
        awaitHistory(1);
        assertThat(mapProducer.history().get(0).key()).isEqualTo(5);
        assertThat(mapProducer.history().get(0).value()).isEqualTo(OWN);
        mapProducer.completeNext();
    }

    @Test
    void 맵_쓰기가_실패하면_다시_보낸다() throws Exception {
        listener.onPartitionsAssigned(partitions(5));
        assignment.acquireAfterLoad(5);
        assignment.confirmAcquired(5);
        awaitHistory(1);

        mapProducer.errorNext(new RuntimeException("브로커 오류"));
        awaitHistory(2);
        mapProducer.completeNext();

        assertThat(mapProducer.history().get(1).value()).isEqualTo(OWN);
        Thread.sleep(150);
        assertThat(mapProducer.history()).as("성공한 뒤에는 더 보내지 않는다").hasSize(2);
    }

    @Test
    void 브로커가_응답하지_않아도_confirmAcquired는_기다리지_않고_돌아온다() throws Exception {
        listener.onPartitionsAssigned(partitions(5));
        assignment.acquireAfterLoad(5);

        long startedAt = System.nanoTime();
        assignment.confirmAcquired(5);
        long elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000L;

        assertThat(elapsedMillis).as("주문 수신 thread가 Kafka 응답을 기다리면 안 된다").isLessThan(500L);
        assertThat(assignment.test(5)).isTrue();
        awaitHistory(1);
        mapProducer.completeNext();
    }

    @Test
    void 맵_쓰기는_쓰기_전용_thread_하나가_넣은_순서대로_보낸다() throws Exception {
        listener.onPartitionsAssigned(partitions(5, 6));
        assignment.acquireAfterLoad(5);
        assignment.acquireAfterLoad(6);

        assignment.confirmAcquired(5);
        assignment.confirmAcquired(6);
        awaitHistory(1);
        Thread.sleep(150);
        assertThat(mapProducer.history()).as("앞 쓰기가 끝나기 전에는 뒤 쓰기를 보내지 않는다").hasSize(1);
        assertThat(mapProducer.history().get(0).key()).isEqualTo(5);

        mapProducer.completeNext();
        awaitHistory(2);
        assertThat(mapProducer.history().get(1).key()).isEqualTo(6);
        mapProducer.completeNext();
    }

    @Test
    void 회수_콜백은_자기_쓰기가_끝날_때까지_기다린다() throws Exception {
        listener.onPartitionsAssigned(partitions(5));
        assignment.acquireAfterLoad(5);
        assignment.confirmAcquired(5);
        awaitHistory(1);
        mapProducer.completeNext();

        java.util.concurrent.atomic.AtomicBoolean returned = new java.util.concurrent.atomic.AtomicBoolean();
        Thread revoke = new Thread(() -> {
            listener.onPartitionsRevoked(partitions(5));
            returned.set(true);
        });
        revoke.start();
        awaitHistory(2);
        Thread.sleep(150);
        assertThat(returned.get()).as("이동 중 표시의 브로커 응답이 오기 전에는 콜백이 끝나지 않는다").isFalse();

        mapProducer.completeNext();
        revoke.join(3000);

        assertThat(returned.get()).isTrue();
        assertThat(mapProducer.history().get(1).value()).isEqualTo(OWN + ";moving");
    }

    @Test
    void 다시_보내기_전에_회수되면_옛_쓰기를_다시_보내지_않는다() throws Exception {
        listener.onPartitionsAssigned(partitions(5));
        assignment.acquireAfterLoad(5);
        assignment.confirmAcquired(5);
        awaitHistory(1);
        assignment.setMapWriteRetryMillis(300L);
        mapProducer.errorNext(new RuntimeException("브로커 오류")); // 300ms 뒤에 다시 보내기가 예약된다

        Thread revoke = new Thread(() -> listener.onPartitionsRevoked(partitions(5)));
        revoke.start();
        while (revoke.isAlive()) {
            mapProducer.completeNext();
            Thread.sleep(5);
        }
        Thread.sleep(500);

        assertThat(mapProducer.history()).extracting(record -> record.value())
            .as("회수 뒤에는 이동 중 표시만 더 올라가고 자기 주소의 평상시 값은 다시 올라가지 않는다")
            .containsExactly(OWN, OWN + ";moving");
    }

    @Test
    void 첫_배정_종목_slot은_담당이_켜져도_맵을_다시_쓰지_않는다() {
        KafkaMatchingShardAssignment first = new KafkaMatchingShardAssignment(mock(Consumer.class),
            new MockProducer<>(true, new IntegerSerializer(), new StringSerializer()), mock(Admin.class), 64, OWN,
            "assignment-topic", "map-topic", new SlotEventRequests(), slot -> Optional.empty(), slot -> { });
        MockProducer<Integer, String> producer = new MockProducer<>(true, new IntegerSerializer(), new StringSerializer());
        KafkaMatchingShardAssignment firstWithProducer = new KafkaMatchingShardAssignment(mock(Consumer.class), producer,
            mock(Admin.class), 64, OWN, "assignment-topic", "map-topic", new SlotEventRequests(), slot -> Optional.empty(), slot -> { });

        firstWithProducer.rebalanceListener().onPartitionsAssigned(partitions(5));
        int afterAssigned = producer.history().size();
        firstWithProducer.confirmAcquired(5);

        assertThat(producer.history()).hasSize(afterAssigned);
        first.close();
        firstWithProducer.close();
    }

    private void awaitHistory(int size) throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (mapProducer.history().size() < size && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(mapProducer.history()).hasSizeGreaterThanOrEqualTo(size);
    }

    private static List<TopicPartition> partitions(int... slots) {
        return java.util.Arrays.stream(slots).mapToObj(slot -> new TopicPartition("assignment-topic", slot)).toList();
    }
}
