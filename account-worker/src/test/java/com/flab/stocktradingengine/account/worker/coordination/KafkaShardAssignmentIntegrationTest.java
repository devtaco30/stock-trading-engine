package com.flab.stocktradingengine.account.worker.coordination;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 계좌 샤딩 U4 — 로컬 실제 브로커(docker-compose)에 붙어 {@link KafkaShardAssignment}가 실제
 * 컨슈머 그룹 배정을 슬롯 판정으로 바꾸는지 확인한다. LLD가 정한 실제 토픽·그룹 이름을 그대로
 * 쓴다({@link KafkaShardAssignment#ASSIGNMENT_TOPIC}가 public 상수로 고정돼 있어 테스트가 다른
 * 이름을 쓸 수 없는 구조) — 워커 신원(group.instance.id)만 테스트 실행마다 다르게 줘서 실제
 * 워커·다른 테스트 실행과 겹치지 않게 한다.
 */
class KafkaShardAssignmentIntegrationTest {

    private static final String BOOTSTRAP = "localhost:9092";
    // run-e2e-v2.sh가 실제로 쓰는 slot-count(256)와 맞춘다 — 다르면 ensureTopicPartitionCount가
    // 기존 토픽과의 불일치로 fail-fast하거나, 이 테스트가 만든 토픽이 실제 값과 어긋나게 남는다.
    private static final int SLOT_COUNT = 256;
    // 반복 실행 시 이전 멤버가 빨리 풀리게 프로덕션 기본값(120000)보다 훨씨 짧게 준다.
    private static final long TEST_SESSION_TIMEOUT_MS = 10_000;

    private KafkaShardAssignment member1;
    private KafkaShardAssignment member2;

    @AfterEach
    void tearDown() {
        if (member1 != null) {
            member1.close();
        }
        if (member2 != null) {
            member2.close();
        }
    }

    @Test
    void 워커_둘이_뜨면_슬롯이_나뉜다() {
        String suffix = String.valueOf(System.nanoTime());
        member1 = newAssignment("test-split-1-" + suffix);
        member1.start();
        awaitSlotCount(member1, SLOT_COUNT, 30);

        member2 = newAssignment("test-split-2-" + suffix);
        member2.start();
        awaitSplit(member1, member2, 60);

        Set<Integer> slots1 = member1.currentSlotsSnapshot();
        Set<Integer> slots2 = member2.currentSlotsSnapshot();
        assertThat(slots1).doesNotContainAnyElementsOf(slots2);
        assertThat(slots1.size() + slots2.size()).isEqualTo(SLOT_COUNT);
    }

    /**
     * static membership은 재배정을 없애는 게 아니라 미룬다 — session.timeout이 지나면 결국
     * 재배정된다(이 테스트는 그 만료 전 구간만 본다). 만료 뒤 실제로 재배정이 일어났을 때
     * "새 슬롯을 받은 워커가 상태 없이 그 계좌를 처리하면 안 된다"는 U6(상태 없는 슬롯 거부)의
     * 몫이고, 이 트랙엔 아직 없다.
     */
    @Test
    void 하나를_내려도_세션_타임아웃_전까지는_그_슬롯을_가져가지_않는다() throws InterruptedException {
        String suffix = String.valueOf(System.nanoTime());
        member1 = newAssignment("test-l1-1-" + suffix);
        member2 = newAssignment("test-l1-2-" + suffix);
        member1.start();
        member2.start();
        awaitSplit(member1, member2, 60);
        int member2SlotsBefore = member2.currentSlotsSnapshot().size();
        assertThat(member2SlotsBefore).isGreaterThan(0);

        member1.close(); // static membership — LeaveGroup을 안 보낸다.
        member1 = null;

        // session.timeout(10초)보다 짧게 기다린 뒤 확인 — L1이면 그 사이엔 재배정이 없어야 한다.
        Thread.sleep(3000);

        assertThat(member2.currentSlotsSnapshot()).hasSize(member2SlotsBefore);
    }

    /**
     * U6 — session.timeout이 진짜로 만료돼 Kafka가 실제로 재배정을 해도(죽은 워커의 슬롯이
     * 남은 워커에게 넘어가도), 남은 워커는 "최초 배정에 없던 슬롯"이라 거부하고 자기 담당
     * 집합을 그대로 유지해야 한다 — L1을 코드로 못 박는 안전망.
     */
    @Test
    void 세션_타임아웃이_지나_재배정돼도_최초_배정에_없던_슬롯은_거부한다() throws InterruptedException {
        String suffix = String.valueOf(System.nanoTime());
        member1 = newAssignment("test-u6-1-" + suffix);
        member2 = newAssignment("test-u6-2-" + suffix);
        member1.start();
        member2.start();
        awaitSplit(member1, member2, 60);
        int member2SlotsBefore = member2.currentSlotsSnapshot().size();
        assertThat(member2SlotsBefore).isGreaterThan(0);

        member1.close(); // static membership — LeaveGroup 없이 종료.
        member1 = null;

        // session.timeout(10초)이 실제로 지날 때까지 기다린 뒤, Kafka가 재배정을 마칠 시간까지 더 준다.
        Thread.sleep(TEST_SESSION_TIMEOUT_MS + 10_000);

        assertThat(member2.currentSlotsSnapshot())
            .as("U6: 최초 배정에 없던 슬롯은 재배정돼도 거부해 담당 집합이 그대로여야 한다")
            .hasSize(member2SlotsBefore);
    }

    private KafkaShardAssignment newAssignment(String instanceId) {
        return new KafkaShardAssignment(
            buildConsumer(instanceId), buildMapProducer(), Admin.create(adminProps()), SLOT_COUNT, "endpoint-of-" + instanceId);
    }

    private Producer<Integer, String> buildMapProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    private void awaitSlotCount(KafkaShardAssignment assignment, int expected, int timeoutSeconds) {
        long deadline = System.nanoTime() + Duration.ofSeconds(timeoutSeconds).toNanos();
        while (assignment.currentSlotsSnapshot().size() != expected) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(timeoutSeconds + "초 안에 슬롯 " + expected + "개를 못 받음: 현재="
                    + assignment.currentSlotsSnapshot().size());
            }
            sleep(200);
        }
    }

    private void awaitSplit(KafkaShardAssignment a, KafkaShardAssignment b, int timeoutSeconds) {
        long deadline = System.nanoTime() + Duration.ofSeconds(timeoutSeconds).toNanos();
        while (true) {
            Set<Integer> slotsA = a.currentSlotsSnapshot();
            Set<Integer> slotsB = b.currentSlotsSnapshot();
            boolean split = slotsA.size() + slotsB.size() == SLOT_COUNT
                && !slotsA.isEmpty() && !slotsB.isEmpty()
                && Collections.disjoint(slotsA, slotsB);
            if (split) {
                return;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError(timeoutSeconds + "초 안에 둘로 안 나뉨: a=" + slotsA.size() + " b=" + slotsB.size());
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

    private Consumer<String, String> buildConsumer(String instanceId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaShardAssignment.GROUP_ID);
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, (int) TEST_SESSION_TIMEOUT_MS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }

    private Properties adminProps() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        return props;
    }
}
