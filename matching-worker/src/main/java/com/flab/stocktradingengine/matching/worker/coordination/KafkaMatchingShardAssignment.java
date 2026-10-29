package com.flab.stocktradingengine.matching.worker.coordination;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.flab.stocktradingengine.aeron.MatchingSlotTarget;
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventRequests;

/**
 * 매칭 shard가 담당할 종목 slot을 Kafka 컨슈머 그룹 배정으로 받는다. 계좌 워커의
 * {@code KafkaShardAssignment}와 같은 방식이다: 조정용 토픽 {@link #ASSIGNMENT_TOPIC}에는 메시지를
 * 한 건도 보내지 않고, 이 매칭 shard가 그룹 {@link #GROUP_ID}에서 배정받은 파티션 번호 집합을 그대로
 * 담당 종목 slot 집합으로 쓴다.
 *
 * <h3>계좌 쪽과 다른 점</h3>
 * <ul>
 * <li>맵 토픽 {@link #MAP_TOPIC}의 값이 주문 입구 주소 하나다(계좌 쪽은 주문 주소와 체결 주소 둘).</li>
 * <li>첫 배정 이후 처음 배정에 없던 종목 slot을 거부하는 안전망을 두지 않는다. 증설로 종목 slot이
 *     옮겨 오는 것이 정상 경로이기 때문이다.</li>
 * <li>컨슈머가 {@link CooperativeStickyAssignor}로 배정을 받는다({@link #consumerProperties}). 지정하지
 *     않으면 기본값(RangeAssignor 우선)이 적용되어 새 멤버가 조인할 때 기존 멤버의 revoke에 64개
 *     전부가 온다. 이 경우 옮겨지지 않는 종목 slot까지 이동 중으로 취급되어 이동 절차가 성립하지
 *     않는다.</li>
 * </ul>
 *
 * <p>기동 직후부터 첫 배정을 받을 때까지 {@link #test}는 모든 종목 slot에서 false다.</p>
 *
 * <h3>담당이 켜지는 시점</h3>
 * <p>요청 통로({@link SlotEventRequests})를 받은 구성에서는 배정 콜백이 종목 slot을 바로 켜지 않는다. 받음 요청만 남기고,
 * 엔진이 받음 이벤트를 저널에 기록한 뒤 {@link #confirmAcquired}를 불러야 켜진다. 켜진 뒤에 들어온 주문은 저널에서 항상
 * 받음 항목 뒤에 있어서, 복구가 그 주문을 걸러내지 않는다(11-12절). 회수 콜백은 즉시 끄고 넘김 요청을 남긴다.
 * 요청 통로가 없는 구성은 배정 즉시 켠다.</p>
 */
public final class KafkaMatchingShardAssignment implements IntPredicate {

    private static final Logger log = System.getLogger(KafkaMatchingShardAssignment.class.getName());

    public static final String ASSIGNMENT_TOPIC = "matching-shard-assignment";
    public static final String GROUP_ID = "matching-shard-owners";
    public static final String MAP_TOPIC = "matching-shard-map";

    private final Consumer<String, String> consumer;
    private final Producer<Integer, String> mapProducer;
    private final Admin adminClient;
    private final int stockSlotCount;
    private final String ownOrderEndpoint;
    private final String assignmentTopic;
    private final String mapTopic;
    // 받음 요청을 남기는 통로. null이면 배정 콜백이 종목 slot을 바로 켠다.
    private final SlotEventRequests slotEventRequests;
    // 받음 요청을 남겼고 저널 기록 확인을 기다리는 종목 slot. 그 사이 회수되면 빠진다.
    private final java.util.Set<Integer> awaitingJournalConfirm = ConcurrentHashMap.newKeySet();

    // test()가 주문마다 불린다. 배열 참조를 volatile로 통째로 교체해 박싱 없이 인덱스 조회만 한다.
    private volatile boolean[] ownedSlotFlags;
    private volatile boolean running;
    private Thread pollThread;

    public KafkaMatchingShardAssignment(Consumer<String, String> consumer, Producer<Integer, String> mapProducer,
                                        Admin adminClient, int stockSlotCount, String ownOrderEndpoint) {
        this(consumer, mapProducer, adminClient, stockSlotCount, ownOrderEndpoint, ASSIGNMENT_TOPIC, MAP_TOPIC);
    }

    /** 토픽 이름을 바꿀 수 있는 생성자. 서로 다른 시험이 같은 브로커를 쓸 때 토픽을 분리하는 데 쓴다. */
    public KafkaMatchingShardAssignment(Consumer<String, String> consumer, Producer<Integer, String> mapProducer,
                                        Admin adminClient, int stockSlotCount, String ownOrderEndpoint,
                                        String assignmentTopic, String mapTopic) {
        this(consumer, mapProducer, adminClient, stockSlotCount, ownOrderEndpoint, assignmentTopic, mapTopic, null);
    }

    /** 받음·넘김 요청 통로를 받는다. 종목 slot 담당은 {@link #confirmAcquired}가 와야 켜진다. */
    public KafkaMatchingShardAssignment(Consumer<String, String> consumer, Producer<Integer, String> mapProducer,
                                        Admin adminClient, int stockSlotCount, String ownOrderEndpoint,
                                        String assignmentTopic, String mapTopic, SlotEventRequests slotEventRequests) {
        this.slotEventRequests = slotEventRequests;
        this.consumer = consumer;
        this.mapProducer = mapProducer;
        this.adminClient = adminClient;
        this.stockSlotCount = stockSlotCount;
        this.ownOrderEndpoint = ownOrderEndpoint;
        this.assignmentTopic = assignmentTopic;
        this.mapTopic = mapTopic;
        this.ownedSlotFlags = new boolean[stockSlotCount];
    }

    /**
     * 이 컨슈머의 설정. {@code group.instance.id}를 주어 정적 멤버십으로 조인하고(재시작해도 같은
     * 신원이면 세션 시간 안에 같은 종목 slot을 돌려받는다), 배정 방식을
     * {@link CooperativeStickyAssignor}로 명시한다.
     */
    public static Properties consumerProperties(String bootstrapServers, String groupId, String groupInstanceId,
                                                int sessionTimeoutMs) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }

    @Override
    public boolean test(int slot) {
        boolean[] flags = ownedSlotFlags;
        return slot >= 0 && slot < flags.length && flags[slot];
    }

    /**
     * 종목 slot의 받음 이벤트가 저널에 기록되었다는 확인. 엔진의 저널 핸들러 thread가 부른다. 그 사이 회수된 종목 slot은
     * 켜지 않는다.
     */
    public void confirmAcquired(int stockSlot) {
        synchronized (this) {
            if (!awaitingJournalConfirm.remove(stockSlot)) {
                return;
            }
            boolean[] updated = ownedSlotFlags.clone();
            updated[stockSlot] = true;
            ownedSlotFlags = updated;
        }
    }

    public Set<Integer> currentSlotsSnapshot() {
        boolean[] flags = ownedSlotFlags;
        return IntStream.range(0, flags.length).filter(slot -> flags[slot]).boxed()
            .collect(Collectors.toUnmodifiableSet());
    }

    public void start() {
        ensureAssignmentTopicPartitionCount();
        ensureMapTopicExists();
        running = true;
        pollThread = new Thread(this::pollLoop, "matching-shard-assignment-poll");
        pollThread.setDaemon(true);
        pollThread.start();
    }

    public void close() {
        running = false;
        consumer.wakeup();
        try {
            if (pollThread != null) {
                pollThread.join(Duration.ofSeconds(5).toMillis());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        consumer.close();
        mapProducer.close();
        adminClient.close();
    }

    private void pollLoop() {
        consumer.subscribe(List.of(assignmentTopic), rebalanceListener());
        try {
            while (running) {
                consumer.poll(Duration.ofMillis(500));
            }
        } catch (WakeupException e) {
            // close()가 poll을 깨운 것이다.
        }
    }

    /**
     * 회수된 종목 slot은 맵 토픽에 이 매칭 shard의 주소에 이동 중 표시를 붙인 값({@code 주소;moving})으로 올리고
     * (지우지 않는다. 새 담당이 이전 담당의 주소를 맵에서 알아야 한다, 10-2절 수정본), 배정된 종목 slot은 이 매칭 shard의 주문 입구 주소로
     * 올린다. 회수 쪽 발행을 동기로({@code get()}) 하는 이유는, 새 담당의 주소 발행보다 이 이동 중 표시가 나중에
     * 브로커에 써져 새 값을 지우는 순서 역전을 막기 위해서다. 협력 방식에서는 두 콜백이 옮겨지는
     * 종목 slot만 받는다.
     */
    ConsumerRebalanceListener rebalanceListener() {
        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                synchronized (KafkaMatchingShardAssignment.this) {
                    boolean[] updated = ownedSlotFlags.clone();
                    partitions.forEach(p -> updated[p.partition()] = false);
                    ownedSlotFlags = updated;
                    partitions.forEach(p -> awaitingJournalConfirm.remove(p.partition()));
                }
                if (slotEventRequests != null) {
                    partitions.forEach(p -> slotEventRequests.requestRelease(p.partition()));
                }
                publishMapEntries(partitions, new MatchingSlotTarget(ownOrderEndpoint, true).encode());
                log.log(Level.WARNING, "[매칭] 담당 종목 slot 회수: " + slotNumbers(partitions)
                    + " 남은 담당=" + currentSlotsSnapshot().size() + "개");
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                if (slotEventRequests == null) {
                    synchronized (KafkaMatchingShardAssignment.this) {
                        boolean[] updated = ownedSlotFlags.clone();
                        partitions.forEach(p -> updated[p.partition()] = true);
                        ownedSlotFlags = updated;
                    }
                } else {
                    partitions.forEach(p -> {
                        awaitingJournalConfirm.add(p.partition());
                        slotEventRequests.requestAcquire(p.partition());
                    });
                }
                if (!partitions.isEmpty()) {
                    publishMapEntries(partitions, ownOrderEndpoint);
                }
                log.log(Level.INFO, "[매칭] 담당 종목 slot 배정: 신규=" + slotNumbers(partitions)
                    + " 전체 담당=" + currentSlotsSnapshot().size() + "개");
            }
        };
    }

    private void publishMapEntries(Collection<TopicPartition> partitions, String value) {
        for (TopicPartition partition : partitions) {
            try {
                mapProducer.send(new ProducerRecord<>(mapTopic, partition.partition(), value)).get();
            } catch (ExecutionException e) {
                throw new IllegalStateException(mapTopic + " 발행 실패: 종목 slot=" + partition.partition() + " value=" + value, e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(mapTopic + " 발행 중 인터럽트됨", e);
            }
        }
    }

    private List<Integer> slotNumbers(Collection<TopicPartition> partitions) {
        return partitions.stream().map(TopicPartition::partition).toList();
    }

    /** 토픽이 없으면 종목 slot 개수와 같은 파티션 수로 만든다. 있는데 파티션 수가 다르면 기동을 막는다. */
    private void ensureAssignmentTopicPartitionCount() {
        try {
            Map<String, TopicDescription> described = adminClient.describeTopics(List.of(assignmentTopic)).allTopicNames().get();
            int actual = described.get(assignmentTopic).partitions().size();
            if (actual != stockSlotCount) {
                throw new IllegalStateException(
                    "조정 토픽 " + assignmentTopic + "의 파티션 수(" + actual + ")가 종목 slot 개수(" + stockSlotCount
                        + ")와 다릅니다 — 설정이 어긋났습니다");
            }
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                createAssignmentTopic();
                return;
            }
            throw new IllegalStateException("조정 토픽 조회 실패: " + assignmentTopic, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("조정 토픽 조회 중 인터럽트됨", e);
        }
    }

    private void createAssignmentTopic() {
        try {
            adminClient.createTopics(List.of(new NewTopic(assignmentTopic, stockSlotCount, (short) 1))).all().get();
            log.log(Level.INFO, "[매칭] 조정 토픽 생성: " + assignmentTopic + " partitions=" + stockSlotCount);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new IllegalStateException("조정 토픽 생성 실패: " + assignmentTopic, e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("조정 토픽 생성 중 인터럽트됨", e);
        }
    }

    private void ensureMapTopicExists() {
        try {
            adminClient.describeTopics(List.of(mapTopic)).allTopicNames().get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                createMapTopic();
                return;
            }
            throw new IllegalStateException(mapTopic + " 조회 실패", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(mapTopic + " 조회 중 인터럽트됨", e);
        }
    }

    /** 맵 토픽은 최신 값만 있으면 되는 KV 저장소라 compacted로, 파티션 1개로 만든다(계좌 쪽과 같다). */
    private void createMapTopic() {
        try {
            NewTopic topic = new NewTopic(mapTopic, 1, (short) 1).configs(Map.of("cleanup.policy", "compact"));
            adminClient.createTopics(List.of(topic)).all().get();
            log.log(Level.INFO, "[매칭] " + mapTopic + " 토픽 생성(compacted)");
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new IllegalStateException(mapTopic + " 생성 실패", e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(mapTopic + " 생성 중 인터럽트됨", e);
        }
    }
}
