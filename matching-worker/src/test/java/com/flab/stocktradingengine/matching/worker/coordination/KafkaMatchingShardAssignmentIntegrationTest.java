package com.flab.stocktradingengine.matching.worker.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.matching.worker.testsupport.MapTopicHistory;

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
    private final List<Thread> confirmers = new ArrayList<>();

    @BeforeEach
    void setUp() {
        suffix = String.valueOf(System.nanoTime());
        assignmentTopic = "test-matching-assignment-" + suffix;
        mapTopic = "test-matching-map-" + suffix;
        groupId = "test-matching-owners-" + suffix;
    }

    @AfterEach
    void tearDown() {
        confirmers.forEach(Thread::interrupt);
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
        // 그룹에서 빠진 둘째의 consumer가 계속 poll하면 곧바로 다시 가입해 일부 종목 slot을 되가져간다. 떠난 멤버는 멈춘 것으로 만든다.
        second.close();

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

    /**
     * U17c. 같은 종목 slot의 맵 쓰기 순서를 실제 Kafka 토픽으로 확인한다. 회수가 낸 이동 중 쓰기가 브로커 오류로 포기되어 queue 뒤로 간 사이
     * 같은 pod가 되받아 낸 평상시 값을 넣으면, 포기됐던 오래된 쓰기는 일련번호가 달라 버려지고 토픽에는 평상시 값만 남아야 한다.
     * 지금까지는 MockProducer 단위 시험만 있었다.
     */
    @Test
    void 포기된_오래된_쓰기는_같은_종목_slot의_더_새로운_쓰기를_실제_토픽에서_덮지_않는다() throws Exception {
        createMapTopic();
        int slot = 5;
        String plain = "order-endpoint-of-m1";
        String moving = plain + ";moving";
        int failureLimit = 2;
        java.util.concurrent.atomic.AtomicInteger movingAttempts = new java.util.concurrent.atomic.AtomicInteger();
        Producer<Integer, String> producer = intercepting(buildMapProducer(), record -> {
            if (record.value().equals(moving) && movingAttempts.incrementAndGet() <= failureLimit) {
                return java.util.concurrent.CompletableFuture.failedFuture(new IllegalStateException("브로커 오류 흉내"));
            }
            return null;
        });
        KafkaMatchingShardAssignment member = newOperationalMember("m1", producer);
        member.setMapWriteFailureLimit(failureLimit);
        member.setMapWriteRetryMillis(1_000L);

        member.enqueueMapWrite(slot, moving, 0);
        awaitCondition("이동 중 쓰기가 실패 한도(" + failureLimit + "번)까지 보내졌다", () -> movingAttempts.get() >= failureLimit);
        // 포기되어 재시도 간격 동안 쉬는 사이에 같은 종목 slot의 더 새로운 쓰기를 넣는다.
        member.enqueueMapWrite(slot, plain, 0);

        awaitCondition("종목 slot의 마지막 값이 평상시 값이다",
            () -> MapTopicHistory.read(BOOTSTRAP, mapTopic).getOrDefault(slot, List.of()).contains(plain));
        // 쓰기 thread는 하나라 포기한 쓰기를 쉬었다가 queue 뒤에 다시 넣은 뒤에야 다음 쓰기(평상시 값)를 처리한다. 평상시 값이 토픽에 보였다면
        // 포기됐던 쓰기는 이미 queue에 있다. 그 쓰기를 처리하고(버리거나 보내고) queue가 빈 채 쓰기 thread가 기다리는 상태가 된 뒤에 단언한다.
        awaitMapWriterIdle();
        assertThat(MapTopicHistory.read(BOOTSTRAP, mapTopic).get(slot))
            .as("포기된 이동 중 쓰기는 보내지지 않아 토픽에는 평상시 값 하나뿐이다").containsExactly(plain);
        assertThat(movingAttempts.get()).as("버려진 쓰기는 실패 한도 뒤로 더 보내지지 않는다").isEqualTo(failureLimit);
    }

    /**
     * U17d. 둘째 멤버를 Admin으로 그룹에서 뺀 뒤 둘째가 lost를 받아도, 둘째가 맵 토픽에 자기 이동 중 표시를 올리지 않는다. 맵 토픽의 최종 값은
     * 도착 순서에 따라 결함이 있어도 맞게 보일 수 있어서 단언은 둘째가 보낸 이동 중 값의 개수에 건다.
     */
    @Test
    void 그룹에서_빠진_둘째가_lost를_받아도_맵_토픽에_둘째의_이동_중_표시를_올리지_않는다() throws Exception {
        String secondPlain = "order-endpoint-of-m2";
        // 둘째가 보낸 모든 값(tombstone이면 null)을 기록한다. 평상시 값이 아닌 값이 하나라도 있으면 회수 콜백이 맵에 쓴 것이다.
        List<String> secondSends = java.util.Collections.synchronizedList(new ArrayList<>());
        Producer<Integer, String> secondProducer = intercepting(buildMapProducer(), record -> {
            secondSends.add(record.value());
            return null;
        });
        // lost 콜백이 불렸다는 직접 신호: 이 클래스가 lost 때만 남기는 WARNING 로그.
        List<String> lostLogs = new java.util.concurrent.CopyOnWriteArrayList<>();
        java.util.logging.Logger assignmentLogger = java.util.logging.Logger.getLogger(KafkaMatchingShardAssignment.class.getName());
        java.util.logging.Handler lostHandler = new java.util.logging.Handler() {
            @Override
            public void publish(java.util.logging.LogRecord logRecord) {
                String message = logRecord.getMessage();
                if (message != null && message.contains("종목 slot을 잃음")) {
                    lostLogs.add(message);
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        assignmentLogger.addHandler(lostHandler);
        KafkaMatchingShardAssignment first = newOperationalMember("m1", buildMapProducer());
        first.start();
        startConfirming(first);
        awaitSlotCount(first, STOCK_SLOT_COUNT);
        KafkaMatchingShardAssignment second = newOperationalMember("m2", secondProducer);
        second.start();
        startConfirming(second);
        awaitSplit(first, second);
        awaitCondition("둘째의 맵 쓰기가 모두 끝났다", () -> second.slotsWaitingForMove().isEmpty() && first.slotsWaitingForMove().isEmpty());
        assertThat(secondSends).as("그룹에서 빼기 전 둘째가 보낸 값은 자기 평상시 값뿐이다").allMatch(secondPlain::equals);

        try (Admin admin = Admin.create(adminProps())) {
            admin.removeMembersFromConsumerGroup(groupId,
                new RemoveMembersFromConsumerGroupOptions(List.of(new MemberToRemove("m2")))).all().get();
        }

        try {
            awaitCondition("둘째가 lost 콜백을 받았다(lost 로그) 또는 회수 콜백을 받았다(평상시가 아닌 값을 보냄)",
                () -> !lostLogs.isEmpty() || secondSends.stream().anyMatch(value -> !secondPlain.equals(value)));
            awaitCondition("첫째가 둘째의 종목 slot까지 받았다 또는 둘째가 다시 가입했다",
                () -> first.currentSlotsSnapshot().size() + second.currentSlotsSnapshot().size() >= STOCK_SLOT_COUNT);
        } finally {
            assignmentLogger.removeHandler(lostHandler);
        }
        assertThat(lostLogs).as("Kafka가 그룹에서 뺀 정적 멤버에게 부른 콜백이 lost여야 한다").isNotEmpty();
        assertThat(secondSends)
            .as("lost를 받은 둘째가 맵에 평상시 값이 아닌 값(이동 중 표시나 tombstone)을 올렸다면 회수 콜백이 불린 것이다")
            .allMatch(secondPlain::equals);
        assertThat(MapTopicHistory.read(BOOTSTRAP, mapTopic).values().stream().flatMap(List::stream))
            .doesNotContain(secondPlain + ";moving");
    }

    /**
     * U17b 결정적 변형. 그룹에서 빠진 첫째(X)가 lost를 받았을 때 X의 이동 중 쓰기가 늦게 나가는 순서를 시험이 정한다: X의 이동 중 쓰기는
     * 시험이 여는 문에 걸려 있고, 둘째(Z)가 종목 slot을 받아 평상시 값을 올린 뒤에 문을 연다. lost가 맵에 쓰지 않으면 문이 열려도 나갈 쓰기가
     * 없어 최종 값은 각 종목 slot의 담당 주소로 남는다. lost가 회수 콜백으로 처리되면 늦게 나간 이동 중 쓰기가 최종 값을 덮어 실패한다.
     */
    @Test
    void 그룹에서_빠진_첫째의_이동_중_쓰기를_늦게_내보내도_맵의_최종_값은_각_종목_slot의_담당_주소다() throws Exception {
        String firstPlain = "order-endpoint-of-m1";
        String firstMoving = firstPlain + ";moving";
        java.util.concurrent.atomic.AtomicBoolean gateArmed = new java.util.concurrent.atomic.AtomicBoolean();
        java.util.concurrent.CountDownLatch gate = new java.util.concurrent.CountDownLatch(1);
        List<String> firstSendsAfterArmed = java.util.Collections.synchronizedList(new ArrayList<>());
        Producer<Integer, String> firstProducer = intercepting(buildMapProducer(), record -> {
            if (gateArmed.get()) {
                firstSendsAfterArmed.add(record.value());
                if (firstMoving.equals(record.value())) {
                    try {
                        gate.await(60, java.util.concurrent.TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            return null;
        });
        KafkaMatchingShardAssignment first = newOperationalMember("m1", firstProducer);
        first.start();
        startConfirming(first);
        awaitSlotCount(first, STOCK_SLOT_COUNT);
        KafkaMatchingShardAssignment second = newOperationalMember("m2", buildMapProducer());
        second.start();
        startConfirming(second);
        awaitSplit(first, second);
        awaitCondition("두 멤버의 맵 쓰기가 모두 끝났다", () -> first.slotsWaitingForMove().isEmpty() && second.slotsWaitingForMove().isEmpty());
        gateArmed.set(true);
        List<String> lostLogs = new java.util.concurrent.CopyOnWriteArrayList<>();
        java.util.logging.Logger assignmentLogger = java.util.logging.Logger.getLogger(KafkaMatchingShardAssignment.class.getName());
        java.util.logging.Handler lostHandler = lostLogHandler(lostLogs);
        assignmentLogger.addHandler(lostHandler);

        try (Admin admin = Admin.create(adminProps())) {
            admin.removeMembersFromConsumerGroup(groupId,
                new RemoveMembersFromConsumerGroupOptions(List.of(new MemberToRemove("m1")))).all().get();
        }
        try {
            awaitCondition("첫째가 lost 콜백을 받았다(lost 로그) 또는 이동 중 값을 보냈다",
                () -> !lostLogs.isEmpty() || firstSendsAfterArmed.contains(firstMoving));
            awaitCondition("두 멤버가 64개를 나눠 맡고 맵 쓰기가 끝났다", () ->
                first.currentSlotsSnapshot().size() + second.currentSlotsSnapshot().size() == STOCK_SLOT_COUNT
                    && first.slotsWaitingForMove().isEmpty() && second.slotsWaitingForMove().isEmpty()
                    && MapTopicHistory.read(BOOTSTRAP, mapTopic).size() == STOCK_SLOT_COUNT);
        } finally {
            assignmentLogger.removeHandler(lostHandler);
        }
        // 둘째가 평상시 값을 올린 뒤에 첫째의 걸려 있던 쓰기를 내보낸다. lost가 맵에 쓰지 않았다면 걸려 있던 쓰기가 없다.
        gate.countDown();
        awaitMapWriterIdle();

        Map<Integer, List<String>> history = MapTopicHistory.read(BOOTSTRAP, mapTopic);
        Set<Integer> firstSlots = first.currentSlotsSnapshot();
        for (int slot = 0; slot < STOCK_SLOT_COUNT; slot++) {
            List<String> values = history.get(slot);
            String last = values.get(values.size() - 1);
            String expectedOwner = firstSlots.contains(slot) ? firstPlain : "order-endpoint-of-m2";
            assertThat(last).as("종목 slot=" + slot + " 기록=" + values + " (마지막 값은 담당 주소여야 한다)").isEqualTo(expectedOwner);
        }
        assertThat(firstSendsAfterArmed)
            .as("그룹에서 빠진 뒤 첫째가 보낸 값은 다시 가입해 새로 받은 종목 slot의 평상시 값뿐이다").allMatch(firstPlain::equals);
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

    /** 운영 열두 인자 생성자로 만든다. 맵 조회는 항상 비어 있어 모든 배정이 첫 배정이고, 요청 통로는 아무것도 하지 않는다. 담당은 {@link #startConfirming}이 켠다. */
    private KafkaMatchingShardAssignment newOperationalMember(String instanceId, Producer<Integer, String> mapProducer) {
        Properties consumerProps = KafkaMatchingShardAssignment.consumerProperties(
            BOOTSTRAP, groupId, instanceId, TEST_SESSION_TIMEOUT_MS);
        KafkaMatchingShardAssignment member = new KafkaMatchingShardAssignment(
            new KafkaConsumer<>(consumerProps), mapProducer, Admin.create(adminProps()),
            STOCK_SLOT_COUNT, "order-endpoint-of-" + instanceId, assignmentTopic, mapTopic,
            new com.flab.stocktradingengine.matching.disruptor.io.SlotEventRequests(),
            slot -> java.util.Optional.empty(), slot -> { }, null, null);
        members.add(member);
        return member;
    }

    /** 엔진이 받음 기록을 저널에 남긴 뒤 부르는 confirmAcquired를 시험이 대신 주기로 부른다(엔진 없이 담당을 켠다). */
    private void startConfirming(KafkaMatchingShardAssignment member) {
        Thread confirmer = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                for (int slot : member.slotsWaitingForMove()) {
                    member.confirmAcquired(slot);
                }
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }, "test-confirmer");
        confirmer.setDaemon(true);
        confirmers.add(confirmer);
        confirmer.start();
    }

    /** 이 클래스가 lost 콜백 때만 남기는 WARNING 로그를 모으는 JUL handler. */
    private java.util.logging.Handler lostLogHandler(List<String> lostLogs) {
        return new java.util.logging.Handler() {
            @Override
            public void publish(java.util.logging.LogRecord logRecord) {
                String message = logRecord.getMessage();
                if (message != null && message.contains("종목 slot을 잃음")) {
                    lostLogs.add(message);
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
    }

    private void createMapTopic() throws Exception {
        try (Admin admin = Admin.create(adminProps())) {
            admin.createTopics(List.of(new NewTopic(mapTopic, 1, (short) 1))).all().get();
        }
    }

    /** {@code send(record)} 호출을 가로채는 Producer. 가로채기 함수가 null을 돌려주면 실제 Producer로 넘긴다. */
    @SuppressWarnings("unchecked")
    private Producer<Integer, String> intercepting(
            Producer<Integer, String> real,
            java.util.function.Function<ProducerRecord<Integer, String>, java.util.concurrent.Future<RecordMetadata>> onSend) {
        return (Producer<Integer, String>) java.lang.reflect.Proxy.newProxyInstance(
            getClass().getClassLoader(), new Class<?>[] {Producer.class}, (proxy, method, args) -> {
                if (method.getName().equals("send") && args != null && args.length == 1) {
                    java.util.concurrent.Future<RecordMetadata> replaced = onSend.apply((ProducerRecord<Integer, String>) args[0]);
                    if (replaced != null) {
                        return replaced;
                    }
                }
                try {
                    return method.invoke(real, args);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    throw e.getCause();
                }
            });
    }

    /** 맵 쓰기 thread(이름 matching-shard-map-writer)가 모두 queue에서 꺼낼 것을 기다리는 상태(WAITING)가 되기를 기다린다. */
    private void awaitMapWriterIdle() {
        awaitCondition("맵 쓰기 thread가 queue가 빈 채 기다리는 상태다", () -> {
            List<Thread> writers = Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().equals("matching-shard-map-writer") && thread.isAlive()).toList();
            return !writers.isEmpty() && writers.stream().allMatch(thread -> thread.getState() == Thread.State.WAITING);
        });
    }

    private void awaitCondition(String description, java.util.function.BooleanSupplier condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("60초 안에 조건이 충족되지 않음: " + description);
            }
            sleep(50);
        }
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

    private Map<Integer, List<String>> readMapTopicHistory() {
        return MapTopicHistory.read(BOOTSTRAP, mapTopic);
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
