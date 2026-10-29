package com.flab.stocktradingengine.matching.worker.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.MemberToRemove;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * 실제 Kafka 브로커에 붙어 {@link KafkaMatchingShardAssignment}가 컨슈머 그룹 배정을 종목 slot 판정과
 * 맵 토픽 발행으로 바꾸는지 확인한다. 브로커 주소는 환경변수 {@code KAFKA_BOOTSTRAP}(기본
 * localhost:9092, docker-compose의 kafka)로 바꿀 수 있다. 시험마다 조정 토픽·맵 토픽·그룹 이름에
 * 접미사를 붙여 서로, 그리고 실제 이름의 토픽과 겹치지 않게 한다.
 *
 * <p>8번 시험 관련 관찰: 배정 방식을 지정하지 않은 같은 구성(클라이언트 3.8.1 기본값)에서는 새 멤버가
 * 조인할 때 기존 멤버의 {@code onPartitionsRevoked}에 보유한 64개 전부가 왔다. 협력 방식으로 명시하면
 * 옮겨 가는 32개만 왔다. 출처는 관찰 시험 1(docs/_observation_tests_result.md, 2026-10-29, 로컬 단일
 * 브로커)이다. 이 파일의 8번 시험은 협력 방식 쪽만 확인하고 기본값 쪽은 다시 돌리지 않는다.</p>
 */
class KafkaMatchingShardAssignmentIntegrationTest {

    private static final String BOOTSTRAP = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
    private static final int STOCK_SLOT_COUNT = 64;
    private static final int TEST_SESSION_TIMEOUT_MS = 10_000;

    private String suffix;
    private String assignmentTopic;
    private String mapTopic;
    private String groupId;
    private final List<KafkaMatchingShardAssignment> members = new ArrayList<>();

    @BeforeEach
    void setUp() {
        suffix = String.valueOf(System.nanoTime());
        assignmentTopic = "test-matching-assignment-" + suffix;
        mapTopic = "test-matching-map-" + suffix;
        groupId = "test-matching-owners-" + suffix;
    }

    @AfterEach
    void tearDown() {
        members.forEach(KafkaMatchingShardAssignment::close);
    }

    @Test
    void 배정_전에는_모든_종목_slot에서_false다() {
        KafkaMatchingShardAssignment member = newMember("m1", assignmentTopic);

        for (int slot = 0; slot < STOCK_SLOT_COUNT; slot++) {
            assertThat(member.test(slot)).as("slot=" + slot).isFalse();
        }
        assertThat(member.currentSlotsSnapshot()).isEmpty();
    }

    @Test
    void 워커_하나가_조인하면_종목_slot_64개_전부_true다() {
        KafkaMatchingShardAssignment member = newMember("m1", assignmentTopic);
        member.start();

        awaitSlotCount(member, STOCK_SLOT_COUNT);

        for (int slot = 0; slot < STOCK_SLOT_COUNT; slot++) {
            assertThat(member.test(slot)).as("slot=" + slot).isTrue();
        }
    }

    @Test
    void 워커_둘이_조인하면_담당이_합쳐_64개이고_겹치지_않는다() {
        KafkaMatchingShardAssignment first = newMember("m1", assignmentTopic);
        first.start();
        awaitSlotCount(first, STOCK_SLOT_COUNT);
        KafkaMatchingShardAssignment second = newMember("m2", assignmentTopic);
        second.start();

        awaitSplit(first, second);

        Set<Integer> firstSlots = first.currentSlotsSnapshot();
        Set<Integer> secondSlots = second.currentSlotsSnapshot();
        assertThat(Collections.disjoint(firstSlots, secondSlots)).isTrue();
        assertThat(firstSlots.size() + secondSlots.size()).isEqualTo(STOCK_SLOT_COUNT);
    }

    @Test
    void 배정되면_맵_토픽에_종목_slot별_주문_입구_주소가_올라온다() {
        KafkaMatchingShardAssignment member = newMember("m1", assignmentTopic);
        member.start();
        awaitSlotCount(member, STOCK_SLOT_COUNT);

        Map<Integer, List<String>> history = awaitMapTopicHistory(
            published -> published.size() == STOCK_SLOT_COUNT);

        for (int slot = 0; slot < STOCK_SLOT_COUNT; slot++) {
            assertThat(history.get(slot)).as("slot=" + slot).containsExactly("order-endpoint-of-m1");
        }
    }

    @Test
    void 회수되면_해당_종목_slot의_값이_이전_담당_주소에_moving을_붙인_값으로_올라온다() {
        KafkaMatchingShardAssignment first = newMember("m1", assignmentTopic);
        first.start();
        awaitSlotCount(first, STOCK_SLOT_COUNT);
        KafkaMatchingShardAssignment second = newMember("m2", assignmentTopic);
        second.start();
        awaitSplit(first, second);

        Set<Integer> movedSlots = second.currentSlotsSnapshot();
        Map<Integer, List<String>> history = awaitMapTopicHistory(
            published -> movedSlots.stream().allMatch(slot -> published.getOrDefault(slot, List.of()).size() == 3));

        for (int slot : movedSlots) {
            assertThat(history.get(slot))
                .as("옮겨 간 slot=" + slot + "은 첫 담당의 주소 → 회수 때 이동 중 표시 → 새 담당의 주소 순서로 쌓인다")
                .containsExactly("order-endpoint-of-m1", "order-endpoint-of-m1;moving", "order-endpoint-of-m2");
        }
    }

    @Test
    void 조정_토픽의_파티션_수가_64가_아니면_기동_예외다() throws Exception {
        try (Admin admin = Admin.create(adminProps())) {
            admin.createTopics(List.of(new NewTopic(assignmentTopic, 32, (short) 1))).all().get();
        }
        KafkaMatchingShardAssignment member = newMember("m1", assignmentTopic);

        assertThatThrownBy(member::start)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("파티션 수(32)")
            .hasMessageContaining("종목 slot 개수(64)");
    }

    @Test
    void 첫_배정을_받은_뒤_새로_배정된_종목_slot도_거부하지_않고_담당으로_등록한다() throws Exception {
        KafkaMatchingShardAssignment first = newMember("m1", assignmentTopic);
        first.start();
        awaitSlotCount(first, STOCK_SLOT_COUNT);
        KafkaMatchingShardAssignment second = newMember("m2", assignmentTopic);
        second.start();
        awaitSplit(first, second);
        assertThat(first.currentSlotsSnapshot().size()).isLessThan(STOCK_SLOT_COUNT);

        // 둘째가 그룹을 떠나면 첫째가 처음 배정에 없던 종목 slot을 새로 받는다.
        try (Admin admin = Admin.create(adminProps())) {
            admin.removeMembersFromConsumerGroup(groupId,
                new RemoveMembersFromConsumerGroupOptions(List.of(new MemberToRemove("m2")))).all().get();
        }

        awaitSlotCount(first, STOCK_SLOT_COUNT);
    }

    @Test
    void 협력_방식이라_새_멤버가_조인하면_기존_멤버는_옮겨_가는_종목_slot만_회수한다() {
        KafkaMatchingShardAssignment first = newMember("m1", assignmentTopic);
        first.start();
        awaitSlotCount(first, STOCK_SLOT_COUNT);
        KafkaMatchingShardAssignment second = newMember("m2", assignmentTopic);
        second.start();
        awaitSplit(first, second);

        Set<Integer> keptSlots = first.currentSlotsSnapshot();
        Set<Integer> movedSlots = second.currentSlotsSnapshot();
        Map<Integer, List<String>> history = awaitMapTopicHistory(
            published -> movedSlots.stream().allMatch(slot -> published.getOrDefault(slot, List.of()).size() == 3));

        Set<Integer> slotsWithMovingRecord = history.entrySet().stream()
            .filter(entry -> entry.getValue().contains("order-endpoint-of-m1;moving"))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        // 회수 콜백이 받은 종목 slot은 맵 토픽에 이동 중 표시(주소;moving)가 한 번 쌓이므로, 그것이 쌓인 종목 slot이 곧 회수된 종목 slot이다.
        assertThat(slotsWithMovingRecord).isEqualTo(movedSlots);
        assertThat(slotsWithMovingRecord).doesNotContainAnyElementsOf(keptSlots);
        assertThat(slotsWithMovingRecord).hasSize(STOCK_SLOT_COUNT - keptSlots.size());
    }

    private KafkaMatchingShardAssignment newMember(String instanceId, String assignmentTopicName) {
        Properties consumerProps = KafkaMatchingShardAssignment.consumerProperties(
            BOOTSTRAP, groupId, instanceId, TEST_SESSION_TIMEOUT_MS);
        KafkaMatchingShardAssignment member = new KafkaMatchingShardAssignment(
            new KafkaConsumer<>(consumerProps), buildMapProducer(), Admin.create(adminProps()),
            STOCK_SLOT_COUNT, "order-endpoint-of-" + instanceId, assignmentTopicName, mapTopic);
        members.add(member);
        return member;
    }

    private Producer<Integer, String> buildMapProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    private Properties adminProps() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        return props;
    }

    /** 맵 토픽을 처음부터 끝까지 읽어 종목 slot별로 쌓인 값을 순서대로 모은다(tombstone은 null). */
    private Map<Integer, List<String>> readMapTopicHistory() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        Map<Integer, List<String>> history = new HashMap<>();
        TopicPartition partition = new TopicPartition(mapTopic, 0);
        try (KafkaConsumer<byte[], String> reader = new KafkaConsumer<>(props,
            new org.apache.kafka.common.serialization.ByteArrayDeserializer(), new StringDeserializer())) {
            reader.assign(List.of(partition));
            reader.seekToBeginning(List.of(partition));
            long endOffset = reader.endOffsets(List.of(partition)).get(partition);
            while (reader.position(partition) < endOffset) {
                ConsumerRecords<byte[], String> records = reader.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], String> record : records) {
                    int slot = java.nio.ByteBuffer.wrap(record.key()).getInt();
                    history.computeIfAbsent(slot, key -> new ArrayList<>()).add(record.value());
                }
            }
        }
        return history;
    }

    /** 담당 표시는 맵 토픽 발행보다 먼저 켜지므로, 발행이 조건을 채울 때까지 맵 토픽을 다시 읽는다. */
    private Map<Integer, List<String>> awaitMapTopicHistory(
        java.util.function.Predicate<Map<Integer, List<String>>> condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        Map<Integer, List<String>> history = readMapTopicHistory();
        while (!condition.test(history)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("30초 안에 맵 토픽이 기대한 상태가 되지 않음: " + history);
            }
            sleep(200);
            history = readMapTopicHistory();
        }
        return history;
    }

    private void awaitSlotCount(KafkaMatchingShardAssignment member, int expected) {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (member.currentSlotsSnapshot().size() != expected) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("60초 안에 종목 slot " + expected + "개를 못 받음: 현재=" + member.currentSlotsSnapshot().size());
            }
            sleep(200);
        }
    }

    private void awaitSplit(KafkaMatchingShardAssignment first, KafkaMatchingShardAssignment second) {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (true) {
            Set<Integer> firstSlots = first.currentSlotsSnapshot();
            Set<Integer> secondSlots = second.currentSlotsSnapshot();
            boolean split = firstSlots.size() + secondSlots.size() == STOCK_SLOT_COUNT
                && !firstSlots.isEmpty() && !secondSlots.isEmpty()
                && Collections.disjoint(firstSlots, secondSlots);
            if (split) {
                return;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("60초 안에 둘로 안 나뉨: first=" + firstSlots.size() + " second=" + secondSlots.size());
            }
            sleep(200);
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("대기 중 인터럽트됨", e);
        }
    }
}
