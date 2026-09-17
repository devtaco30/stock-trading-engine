package com.flab.stocktradingengine.aeron;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 계좌 샤딩 U5 — 로컬 실제 브로커(docker-compose)에 붙어 {@link AssignmentDestinationResolver}가
 * {@code account-shard-map}(compacted)에 발행된 배정·tombstone을 실제로 반영하는지 확인한다.
 * 이 토픽은 {@code account-worker}의 {@code KafkaShardAssignment}가 실제로 쓰는 이름과 같다
 * ({@link #MAP_TOPIC}) — 발행자를 직접 흉내 내(raw producer) 소비자만 단위로 검증한다.
 */
class AssignmentDestinationResolverIntegrationTest {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final String MAP_TOPIC = AssignmentDestinationResolver.MAP_TOPIC;
    private static final int SLOT_COUNT = 256;

    private AssignmentDestinationResolver resolver;
    private Producer<Integer, String> producer;

    @AfterEach
    void tearDown() {
        if (resolver != null) {
            resolver.close();
        }
        if (producer != null) {
            producer.close();
        }
    }

    @Test
    void 발행된_배정을_읽어_목적지를_돌려준다() {
        ensureMapTopicExists();
        ShardRoutingTable routingTable = new ShardRoutingTable(SLOT_COUNT, List.of(
            new ShardRoutingTable.ShardRange("placeholder", 0, SLOT_COUNT - 1)));
        long accountId = 1L;
        int slot = routingTable.slotFor(accountId);
        String endpoint = "aeron:udp?endpoint=localhost:" + (30000 + slot);

        producer = buildProducer();
        producer.send(new ProducerRecord<>(MAP_TOPIC, slot, endpoint));
        producer.flush();

        resolver = new AssignmentDestinationResolver(buildConsumer(), routingTable, SLOT_COUNT);
        resolver.start();

        awaitEndpoint(accountId, Optional.of(endpoint), 30);
    }

    @Test
    void tombstone을_받으면_목적지가_다시_비워진다() {
        ensureMapTopicExists();
        ShardRoutingTable routingTable = new ShardRoutingTable(SLOT_COUNT, List.of(
            new ShardRoutingTable.ShardRange("placeholder", 0, SLOT_COUNT - 1)));
        long accountId = 2L;
        int slot = routingTable.slotFor(accountId);
        String endpoint = "aeron:udp?endpoint=localhost:" + (31000 + slot);

        producer = buildProducer();
        producer.send(new ProducerRecord<>(MAP_TOPIC, slot, endpoint));
        producer.flush();

        resolver = new AssignmentDestinationResolver(buildConsumer(), routingTable, SLOT_COUNT);
        resolver.start();
        awaitEndpoint(accountId, Optional.of(endpoint), 30);

        producer.send(new ProducerRecord<>(MAP_TOPIC, slot, null));
        producer.flush();

        awaitEndpoint(accountId, Optional.empty(), 30);
    }

    /**
     * 계좌 샤딩 U6 — awaitInitialCatchUp이 돌아온 시점엔 그 전에 이미 발행돼 있던 기록이 전부
     * 반영돼 있어야 한다("아직 못 읽었는데 준비됐다고 알림" 방지). 매칭의
     * {@code MatchingOrderReceiverLifecycle}이 이 보장에 기대 주문 인테이크 구독 시점을 정한다.
     */
    @Test
    void 초기_읽기가_끝나면_그_전에_발행된_배정이_이미_반영돼_있다() {
        ensureMapTopicExists();
        ShardRoutingTable routingTable = new ShardRoutingTable(SLOT_COUNT, List.of(
            new ShardRoutingTable.ShardRange("placeholder", 0, SLOT_COUNT - 1)));
        long accountId = 3L;
        int slot = routingTable.slotFor(accountId);
        String endpoint = "aeron:udp?endpoint=localhost:" + (32000 + slot);

        producer = buildProducer();
        producer.send(new ProducerRecord<>(MAP_TOPIC, slot, endpoint));
        producer.flush();

        resolver = new AssignmentDestinationResolver(buildConsumer(), routingTable, SLOT_COUNT);
        resolver.start();
        resolver.awaitInitialCatchUp(Duration.ofSeconds(30));

        // await 없이 바로 확인 — 이미 반영돼 있어야 한다.
        assertEquals(Optional.of(endpoint), resolver.endpointFor(accountId));
    }

    private void awaitEndpoint(long accountId, Optional<String> expected, int timeoutSeconds) {
        long deadline = System.nanoTime() + Duration.ofSeconds(timeoutSeconds).toNanos();
        Optional<String> last = Optional.empty();
        while (true) {
            last = resolver.endpointFor(accountId);
            if (last.equals(expected)) {
                return;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError(timeoutSeconds + "초 안에 기대한 목적지가 안 됨: 기대=" + expected + " 실제=" + last);
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

    private Producer<Integer, String> buildProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    private Consumer<Integer, String> buildConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    private void ensureMapTopicExists() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        try (Admin admin = Admin.create(props)) {
            admin.createTopics(List.of(new NewTopic(MAP_TOPIC, 1, (short) 1))).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new IllegalStateException("account-shard-map 생성 실패", e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("account-shard-map 생성 중 인터럽트됨", e);
        }
    }
}
