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
        ThrowingProducer() {
            super(true, new IntegerSerializer(), new StringSerializer());
        }

        @Override
        public synchronized Future<RecordMetadata> send(ProducerRecord<Integer, String> record) {
            throw new IllegalStateException("직렬화 실패 흉내");
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
        IntFunction<Optional<MatchingSlotTarget>> lookup = slot -> Optional.of(new MatchingSlotTarget(PREVIOUS_OWNER, true));
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
