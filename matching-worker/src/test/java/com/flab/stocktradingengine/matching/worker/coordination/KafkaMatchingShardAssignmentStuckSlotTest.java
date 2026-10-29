package com.flab.stocktradingengine.matching.worker.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.function.IntFunction;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.MatchingSlotTarget;
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventRequests;

/**
 * 이동이 멈춘 종목 slot이 로그와 조회 메서드로 드러나는지. 담당 표시를 켠 뒤 맵에 자기 주소를 올리는 쓰기가 예외로 끝나거나, 이동으로 받는지
 * 판정하지 못하고 끝난 종목 slot은 자동으로 풀리지 않으므로 어느 종목 slot이 멈췄는지 ERROR 로그와 개수로 확인할 수 있어야 한다.
 */
class KafkaMatchingShardAssignmentStuckSlotTest {

    private static final String OWN = "aeron:udp?endpoint=localhost:20021";
    private static final String PREVIOUS_OWNER = "aeron:udp?endpoint=localhost:20020";

    /** 보내는 호출 자체가 예외를 던지는 producer(재시도하지 않는 RuntimeException 경로). */
    private static final class ThrowingProducer extends MockProducer<Integer, String> {
        private final java.util.concurrent.atomic.AtomicInteger failuresLeft;
        // 정해지면 이 종목 slot의 쓰기만 항상 예외를 던진다(입력 문제로 영구히 실패하는 쓰기).
        volatile Integer alwaysThrowForSlot;
        // 정해지면 이 종목 slot의 쓰기는 브로커 오류로 끝나는 future를 돌려준다(보내는 호출은 성공하고 응답이 실패).
        volatile Integer brokerFailForSlot;
        // 정해지면 이 접미사로 끝나는 값의 쓰기는 브로커 오류로 끝난다(예: 회수가 올리는 「;moving」).
        volatile String brokerFailValueSuffix;

        ThrowingProducer() {
            this(Integer.MAX_VALUE);
        }

        ThrowingProducer(int failures) {
            super(true, new IntegerSerializer(), new StringSerializer());
            this.failuresLeft = new java.util.concurrent.atomic.AtomicInteger(failures);
        }

        @Override
        public synchronized Future<RecordMetadata> send(ProducerRecord<Integer, String> record) {
            if (alwaysThrowForSlot != null && alwaysThrowForSlot.equals(record.key())) {
                throw new IllegalStateException("이 쓰기는 영구히 실패");
            }
            if (brokerFailValueSuffix != null && record.value().endsWith(brokerFailValueSuffix)) {
                return java.util.concurrent.CompletableFuture.failedFuture(new IllegalStateException("브로커 오류 흉내"));
            }
            if (brokerFailForSlot != null && brokerFailForSlot.equals(record.key())) {
                return java.util.concurrent.CompletableFuture.failedFuture(new IllegalStateException("브로커 오류 흉내"));
            }
            if (failuresLeft.getAndDecrement() > 0) {
                throw new IllegalStateException("직렬화 실패 흉내");
            }
            return super.send(record);
        }
    }

    private final List<LogRecord> errors = new CopyOnWriteArrayList<>();
    private final Handler handler = new Handler() {
        @Override
        public void publish(LogRecord record) {
            if (record.getLevel().intValue() >= Level.SEVERE.intValue()) {
                errors.add(record);
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    };
    private final Logger julLogger = Logger.getLogger(KafkaMatchingShardAssignment.class.getName());
    private KafkaMatchingShardAssignment assignment;
    private MockProducer<Integer, String> assignmentProducer;
    private ConsumerRebalanceListener listener;

    @BeforeEach
    void setUp() {
        julLogger.addHandler(handler);
    }

    @AfterEach
    void tearDown() {
        julLogger.removeHandler(handler);
        if (assignment != null) {
            assignment.close();
        }
    }

    private void build(MockProducer<Integer, String> producer, MapSync mapSync) {
        build(producer, mapSync, slot -> Optional.of(new MatchingSlotTarget(PREVIOUS_OWNER, true)));
    }

    private void build(MockProducer<Integer, String> producer, MapSync mapSync, IntFunction<Optional<MatchingSlotTarget>> lookup) {
        assignmentProducer = producer;
        assignment = new KafkaMatchingShardAssignment(mock(Consumer.class), producer, mock(Admin.class), 64, OWN,
            "assignment-topic", "map-topic", new SlotEventRequests(), lookup, slot -> { }, mapSync, Runnable::run);
        listener = assignment.rebalanceListener();
    }

    @Test
    void 자기_주소를_맵에_올리는_쓰기가_예외로_끝나면_종목_slot_번호가_담긴_ERROR_로그를_남긴다() throws Exception {
        build(new ThrowingProducer(), () -> true);
        listener.onPartitionsAssigned(partitions(5));
        assignment.acquireAfterLoad(5);

        assignment.confirmAcquired(5);

        awaitErrors();
        assertThat(errors.get(0).getMessage()).contains("종목 slot=5").contains("맵");
    }

    @Test
    void 쓰기가_예외로_끝나도_다시_보낸다() throws Exception {
        ThrowingProducer producer = new ThrowingProducer(1);
        build(producer, () -> true);
        assignment.setMapWriteRetryMillis(30L);
        listener.onPartitionsAssigned(partitions(5));
        assignment.acquireAfterLoad(5);

        assignment.confirmAcquired(5);

        long deadline = System.nanoTime() + 3_000_000_000L;
        while (producer.history().isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(producer.history()).hasSize(1);
        assertThat(producer.history().get(0).value()).isEqualTo(OWN);
        assertThat(errors).isNotEmpty();
    }

    @Test
    void 한_쓰기가_계속_예외를_던져도_뒤의_종목_slot의_쓰기는_나간다() throws Exception {
        ThrowingProducer producer = new ThrowingProducer(0);
        producer.alwaysThrowForSlot = 5;
        build(producer, () -> true);
        assignment.setMapWriteRetryMillis(10L);
        listener.onPartitionsAssigned(partitions(5, 6));
        assignment.acquireAfterLoad(5);
        assignment.acquireAfterLoad(6);

        assignment.confirmAcquired(5);
        assignment.confirmAcquired(6);

        long deadline = System.nanoTime() + 3_000_000_000L;
        while (producer.history().stream().noneMatch(record -> record.key() == 6) && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(producer.history()).anyMatch(record -> record.key() == 6);
        assertThat(errors.stream().anyMatch(record -> record.getMessage().contains("포기"))).isTrue();
    }

    @Test
    void 맵_전환_쓰기를_포기하면_담당은_켜졌어도_멈춘_개수에_센다() throws Exception {
        ThrowingProducer producer = new ThrowingProducer(0);
        producer.alwaysThrowForSlot = 5;
        build(producer, () -> true);
        assignment.setMapWriteRetryMillis(10L);
        listener.onPartitionsAssigned(partitions(5));
        assignment.acquireAfterLoad(5);
        assignment.confirmAcquired(5);

        long deadline = System.nanoTime() + 3_000_000_000L;
        while (assignment.stuckSlotCount() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }

        assertThat(assignment.test(5)).as("담당 표시는 켜졌다").isTrue();
        assertThat(assignment.stuckSlotCount()).as("맵이 이전 담당의 이동 중 값으로 남은 종목 slot을 센다").isEqualTo(1);
    }

    @Test
    void 포기한_맵_전환_쓰기의_종목_slot이_회수되면_멈춘_개수에서_빠진다() throws Exception {
        ThrowingProducer producer = new ThrowingProducer(0);
        producer.alwaysThrowForSlot = 5;
        build(producer, () -> true);
        assignment.setMapWriteRetryMillis(10L);
        listener.onPartitionsAssigned(partitions(5));
        assignment.acquireAfterLoad(5);
        assignment.confirmAcquired(5);
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (assignment.stuckSlotCount() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        producer.alwaysThrowForSlot = null;

        listener.onPartitionsRevoked(partitions(5));

        assertThat(assignment.stuckSlotCount()).isZero();
    }

    @Test
    void 한_쓰기가_브로커_오류로_계속_실패해도_뒤의_종목_slot의_쓰기는_나가고_그_쓰기는_멈춘_개수에_센다() throws Exception {
        ThrowingProducer producer = new ThrowingProducer(0);
        producer.brokerFailForSlot = 5;
        build(producer, () -> true);
        assignment.setMapWriteRetryMillis(10L);
        assignment.setMapWriteFailureLimit(3);
        listener.onPartitionsAssigned(partitions(5, 6));
        assignment.acquireAfterLoad(5);
        assignment.acquireAfterLoad(6);

        assignment.confirmAcquired(5);
        assignment.confirmAcquired(6);

        long deadline = System.nanoTime() + 3_000_000_000L;
        while ((producer.history().stream().noneMatch(record -> record.key() == 6) || assignment.stuckSlotCount() == 0)
            && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(producer.history()).as("앞 쓰기가 계속 실패해도 뒤 종목 slot의 쓰기는 나간다").anyMatch(record -> record.key() == 6);
        assertThat(assignment.stuckSlotCount()).isEqualTo(1);
        assertThat(errors.stream().anyMatch(record -> record.getMessage().contains("종목 slot=5"))).isTrue();
    }

    @Test
    void queue_뒤로_보낸_쓰기가_나중에_성공하면_멈춘_개수에서_빠진다() throws Exception {
        ThrowingProducer producer = new ThrowingProducer(0);
        producer.brokerFailForSlot = 5;
        build(producer, () -> true);
        assignment.setMapWriteRetryMillis(10L);
        assignment.setMapWriteFailureLimit(3);
        listener.onPartitionsAssigned(partitions(5));
        assignment.acquireAfterLoad(5);
        assignment.confirmAcquired(5);
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (assignment.stuckSlotCount() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(assignment.stuckSlotCount()).isEqualTo(1);

        producer.brokerFailForSlot = null; // 브로커가 돌아왔다

        deadline = System.nanoTime() + 3_000_000_000L;
        while (assignment.stuckSlotCount() != 0 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(assignment.stuckSlotCount()).isZero();
        assertThat(producer.history()).anyMatch(record -> record.key() == 5);
    }

    @Test
    void 뒤로_밀린_회수_쓰기가_나중에_성공해도_같은_종목_slot의_더_새로운_쓰기를_덮어쓰지_않는다() throws Exception {
        ThrowingProducer producer = new ThrowingProducer(0);
        producer.brokerFailValueSuffix = ";moving";
        build(producer, () -> true);
        assignment.setMapWriteRetryMillis(10L);
        assignment.setMapWriteFailureLimit(3);
        assignment.setMapWriteDeadlineMillis(100L);
        listener.onPartitionsAssigned(partitions(5));
        assignment.acquireAfterLoad(5);
        assignment.confirmAcquired(5); // 평상시 값 W1
        awaitHistoryKeys(5, 1);

        listener.onPartitionsRevoked(partitions(5)); // 「;moving」 W2: 브로커 오류로 계속 실패한다
        listener.onPartitionsAssigned(partitions(5)); // 같은 pod가 되받는다
        assignment.acquireAfterLoad(5);
        assignment.confirmAcquired(5); // 평상시 값 W3
        awaitHistoryKeys(5, 2);
        producer.brokerFailValueSuffix = null; // 브로커 오류가 풀린다
        Thread.sleep(300);

        List<String> valuesForFive = producer.history().stream().filter(record -> record.key() == 5)
            .map(ProducerRecord::value).toList();
        assertThat(valuesForFive.get(valuesForFive.size() - 1)).as("마지막으로 쓴 값이 담당 pod 자신의 평상시 값이다").isEqualTo(OWN);
        assertThat(valuesForFive).as("옛 「;moving」 쓰기는 더 새로운 쓰기가 있으면 버려진다").noneMatch(value -> value.endsWith(";moving"));
        assertThat(assignment.stuckSlotCount()).as("새 쓰기가 성공했으니 맵은 맞고 멈춘 것이 아니다").isZero();
        assertThat(assignment.slotsWaitingForMove()).as("버려진 옛 쓰기와 성공한 새 쓰기 모두 대기에서 빠진다").isEmpty();
    }

    @Test
    void 첫_배정으로_판정된_종목_slot도_저널_기록_확인을_기다리는_동안_이동_대기에_센다() {
        build(new MockProducer<>(true, new IntegerSerializer(), new StringSerializer()), () -> true, slot -> Optional.empty());

        listener.onPartitionsAssigned(partitions(5));

        assertThat(assignment.slotsWaitingForMove()).containsExactly(5);
        assignment.confirmAcquired(5);
        assertThat(assignment.slotsWaitingForMove()).isEmpty();
    }

    @Test
    void 맵_전환_쓰기가_queue에서_실패를_반복하는_동안에도_이동_대기에_센다() throws Exception {
        ThrowingProducer producer = new ThrowingProducer(0);
        producer.brokerFailForSlot = 5;
        build(producer, () -> true);
        assignment.setMapWriteRetryMillis(10L);
        assignment.setMapWriteFailureLimit(1_000);
        listener.onPartitionsAssigned(partitions(5));
        assignment.acquireAfterLoad(5);

        assignment.confirmAcquired(5); // awaitingMapSwitch는 비워지지만 쓰기는 아직 끝나지 않았다
        Thread.sleep(100);

        assertThat(assignment.slotsWaitingForMove()).containsExactly(5);
    }

    @Test
    void 쓰기가_성공하면_이동_대기에서_빠진다() throws Exception {
        build(new ThrowingProducer(0), () -> true);
        assignment.setMapWriteRetryMillis(10L);
        listener.onPartitionsAssigned(partitions(5));
        assignment.acquireAfterLoad(5);

        assignment.confirmAcquired(5);

        long deadline = System.nanoTime() + 3_000_000_000L;
        while (!assignment.slotsWaitingForMove().isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(assignment.slotsWaitingForMove()).isEmpty();
    }

    @Test
    void 쓰기_thread가_다른_종목_slot에서_막혀_있어도_잃은_종목_slot의_밀린_쓰기는_이동_대기에서_빠진다() throws Exception {
        ThrowingProducer producer = new ThrowingProducer(0);
        producer.brokerFailForSlot = 7;
        build(producer, () -> true);
        assignment.setMapWriteRetryMillis(10L);
        assignment.setMapWriteFailureLimit(1_000_000);
        listener.onPartitionsAssigned(partitions(5, 7));
        assignment.acquireAfterLoad(5);
        assignment.acquireAfterLoad(7);
        assignment.confirmAcquired(7); // 이 쓰기가 계속 실패하며 쓰기 thread를 붙잡는다
        assignment.confirmAcquired(5); // 5의 쓰기는 7의 뒤에 queue에서 기다린다
        Thread.sleep(100);
        assertThat(assignment.slotsWaitingForMove()).containsExactlyInAnyOrder(5, 7);

        listener.onPartitionsLost(partitions(5));

        assertThat(assignment.slotsWaitingForMove()).as("이미 담당이 아닌 종목 slot을 멈춤으로 세지 않는다").containsExactly(7);
    }

    @Test
    void 맵_읽기가_따라오지_못해_판정하지_못하면_종목_slot_번호가_담긴_ERROR_로그를_남기고_멈춘_개수에_센다() {
        build(new MockProducer<>(true, new IntegerSerializer(), new StringSerializer()), () -> false);

        listener.onPartitionsAssigned(partitions(5, 6));

        assertThat(errors).anySatisfy(record -> assertThat(record.getMessage()).contains("5").contains("6"));
        assertThat(assignment.stuckSlotCount()).isEqualTo(2);
    }

    @Test
    void 호가창_적재를_기다리는_종목_slot도_멈춘_개수에_센다() {
        build(new MockProducer<>(true, new IntegerSerializer(), new StringSerializer()), () -> true);

        listener.onPartitionsAssigned(partitions(5));

        assertThat(assignment.slotsAwaitingLoad()).containsExactly(5);
        assertThat(assignment.stuckSlotCount()).isEqualTo(1);
        assignment.acquireAfterLoad(5);
        assertThat(assignment.stuckSlotCount()).isZero();
    }

    private void awaitHistoryKeys(int key, int count) throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (producer(key, count) && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
    }

    private boolean producer(int key, int count) {
        return ((MockProducer<Integer, String>) assignmentProducer).history().stream().filter(record -> record.key() == key).count() < count;
    }

    private void awaitErrors() throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (errors.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(errors).as("3초 안에 ERROR 로그가 남지 않음").isNotEmpty();
    }

    private static List<TopicPartition> partitions(int... slots) {
        return java.util.Arrays.stream(slots).mapToObj(slot -> new TopicPartition("assignment-topic", slot)).toList();
    }
}
