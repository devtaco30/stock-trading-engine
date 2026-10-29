package com.flab.stocktradingengine.matching.worker.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.context.support.GenericApplicationContext;

import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventRequests;
import com.flab.stocktradingengine.matching.worker.lifecycle.KafkaMatchingShardAssignmentLifecycle;
import com.flab.stocktradingengine.matching.worker.lifecycle.MatchingSnapshotLifecycle;
import com.flab.stocktradingengine.matching.worker.recovery.MatchingSnapshotStore;
import com.flab.stocktradingengine.matching.worker.recovery.StoredMatchingSnapshot;

/**
 * 정상 종료(Spring 컨텍스트 close)로 쓰인 스냅샷의 「맡은 종목 slot 목록」. Kafka 컨슈머는 닫힐 때 맡은 파티션 전부에
 * 대해 회수 콜백을 부르므로, 배정 생명주기가 스냅샷 생명주기보다 먼저 멈추면 스냅샷이 쓰일 때 목록이 이미 비어 있다.
 * 컨슈머의 close()가 회수 콜백을 부르는 동작은 mock으로 흉내 낸다.
 */
class GracefulStopOwnedSlotsSnapshotTest {

    private static final int STOCK_SLOT_COUNT = 64;
    private static final Set<Integer> OWNED = Set.of(3, 5, 9);

    @TempDir
    Path archiveDir;

    @Test
    void 스냅샷_생명주기가_먼저_등록되어도_스냅샷의_담당_목록은_종료_전_목록이다() {
        assertThat(stopAndReadOwnedSlots(true)).isEqualTo(OWNED);
    }

    @Test
    void 배정_생명주기가_먼저_등록되어도_스냅샷의_담당_목록은_종료_전_목록이다() {
        assertThat(stopAndReadOwnedSlots(false)).isEqualTo(OWNED);
    }

    private Set<Integer> stopAndReadOwnedSlots(boolean snapshotRegisteredFirst) {
        MatchingEngine engine = new MatchingEngine(1024, (stockCode, fill) -> { });
        engine.start();
        SlotEventRequests requests = new SlotEventRequests();
        AtomicReference<ConsumerRebalanceListener> listenerRef = new AtomicReference<>();
        Consumer<String, String> consumer = mock(Consumer.class);
        doAnswer(invocation -> {
            List<TopicPartition> all = OWNED.stream().map(slot -> new TopicPartition("assignment", slot)).toList();
            listenerRef.get().onPartitionsRevoked(all);
            return null;
        }).when(consumer).close();
        KafkaMatchingShardAssignment assignment = new KafkaMatchingShardAssignment(consumer,
            new MockProducer<>(true, new IntegerSerializer(), new StringSerializer()), adminWithTopics(),
            STOCK_SLOT_COUNT, "order-endpoint", "assignment-topic", "map-topic", requests);
        listenerRef.set(assignment.rebalanceListener());
        listenerRef.get().onPartitionsAssigned(OWNED.stream().map(slot -> new TopicPartition("assignment", slot)).toList());
        OWNED.forEach(assignment::confirmAcquired);

        IntPredicate ownedSlots = assignment;
        MatchingSnapshotStore store = new MatchingSnapshotStore(new File(archiveDir.toString()),
            () -> IntStream.range(0, STOCK_SLOT_COUNT).filter(ownedSlots).boxed().collect(Collectors.toUnmodifiableSet()));
        MatchingSnapshotLifecycle snapshotLifecycle = new MatchingSnapshotLifecycle(engine, store, 7L);
        KafkaMatchingShardAssignmentLifecycle assignmentLifecycle = new KafkaMatchingShardAssignmentLifecycle(assignment);

        GenericApplicationContext context = new GenericApplicationContext();
        if (snapshotRegisteredFirst) {
            context.registerBean("snapshotLifecycle", MatchingSnapshotLifecycle.class, () -> snapshotLifecycle);
            context.registerBean("assignmentLifecycle", KafkaMatchingShardAssignmentLifecycle.class, () -> assignmentLifecycle);
        } else {
            context.registerBean("assignmentLifecycle", KafkaMatchingShardAssignmentLifecycle.class, () -> assignmentLifecycle);
            context.registerBean("snapshotLifecycle", MatchingSnapshotLifecycle.class, () -> snapshotLifecycle);
        }
        context.refresh();
        engine.shutdown();
        context.close();

        StoredMatchingSnapshot stored = store.read().orElseThrow();
        return stored.ownedStockSlots();
    }

    /** start()가 확인하는 조정 토픽(파티션 64개)과 맵 토픽이 이미 있는 것처럼 답하는 Admin. */
    private static Admin adminWithTopics() {
        List<TopicPartitionInfo> partitions = IntStream.range(0, STOCK_SLOT_COUNT)
            .mapToObj(partition -> new TopicPartitionInfo(partition, Node.noNode(), List.of(), List.of())).toList();
        TopicDescription description = new TopicDescription("assignment-topic", false, partitions);
        DescribeTopicsResult result = mock(DescribeTopicsResult.class);
        when(result.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of("assignment-topic", description)));
        Admin admin = mock(Admin.class);
        when(admin.describeTopics(anyCollection())).thenReturn(result);
        return admin;
    }
}
