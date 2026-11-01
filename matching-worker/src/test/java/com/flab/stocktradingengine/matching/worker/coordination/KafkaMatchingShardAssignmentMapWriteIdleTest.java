package com.flab.stocktradingengine.matching.worker.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Optional;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.matching.disruptor.io.SlotEventRequests;

/**
 * 맵 쓰기 thread가 넣어 둔 쓰기를 모두 끝냈는지 직접 본다. thread 상태(WAITING)로는 브로커 응답을 기다리는 중과 queue가 빈 채 기다리는 중을 구분할 수 없고,
 * 포기한 쓰기를 쉬었다가 queue 뒤에 다시 넣는 사이도 놓친다.
 */
class KafkaMatchingShardAssignmentMapWriteIdleTest {

    private final MockProducer<Integer, String> producer = new MockProducer<>(false, new IntegerSerializer(), new StringSerializer());
    private KafkaMatchingShardAssignment assignment;

    @AfterEach
    void tearDown() {
        if (assignment != null) {
            assignment.close();
        }
    }

    private KafkaMatchingShardAssignment newAssignment() {
        assignment = new KafkaMatchingShardAssignment(mock(Consumer.class), producer, mock(Admin.class), 64, "own",
            "assignment-topic", "map-topic", new SlotEventRequests(), slot -> Optional.empty(), slot -> { }, null, null);
        return assignment;
    }

    @Test
    void 넣은_쓰기가_브로커_응답을_기다리는_동안은_한가하지_않고_응답이_오면_한가하다() throws Exception {
        KafkaMatchingShardAssignment member = newAssignment();
        assertThat(member.mapWritesIdle()).as("넣은 쓰기가 없으면 한가하다").isTrue();

        member.enqueueMapWrite(5, "a", 0);
        assertThat(member.mapWritesIdle()).as("넣자마자 한가하지 않다").isFalse();
        awaitUntil(() -> producer.history().size() == 1);
        assertThat(member.mapWritesIdle()).as("쓰기 thread가 꺼내 응답을 기다리는 중에도 한가하지 않다").isFalse();

        producer.completeNext();
        awaitUntil(member::mapWritesIdle);
    }

    @Test
    void 포기한_쓰기를_쉬었다가_queue_뒤에_다시_넣는_사이에도_한가하지_않다() throws Exception {
        KafkaMatchingShardAssignment member = newAssignment();
        member.setMapWriteFailureLimit(1);
        member.setMapWriteRetryMillis(300L);

        member.enqueueMapWrite(5, "a", 0);
        awaitUntil(() -> producer.history().size() == 1);
        producer.errorNext(new RuntimeException("브로커 오류 흉내"));

        Thread.sleep(100); // 포기하고 쉬는 중이다
        assertThat(member.mapWritesIdle()).as("쉬었다가 다시 넣을 쓰기가 남아 있다").isFalse();
        awaitUntil(() -> producer.history().size() == 2);
        producer.completeNext();
        awaitUntil(member::mapWritesIdle);
    }

    @Test
    void 더_새로운_쓰기에_밀려_버려지는_오래된_쓰기도_끝난_것으로_센다() throws Exception {
        KafkaMatchingShardAssignment member = newAssignment();
        member.enqueueMapWrite(5, "old", 0);
        member.enqueueMapWrite(5, "new", 0);
        awaitUntil(() -> producer.history().size() >= 1);

        // 첫 쓰기는 이미 꺼내졌을 수 있다. 응답을 모두 내리면서 둘 다 끝나기를 기다린다.
        for (int i = 0; i < 4 && !member.mapWritesIdle(); i++) {
            producer.completeNext();
            Thread.sleep(50);
        }

        awaitUntil(member::mapWritesIdle);
    }

    private static void awaitUntil(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(condition.getAsBoolean()).as("5초 안에 조건이 충족되지 않음").isTrue();
    }
}
