package com.flab.stocktradingengine.account.worker.coordination;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.WakeupException;

import com.flab.stocktradingengine.aeron.AssignmentDestinationResolver;
import com.flab.stocktradingengine.aeron.WorkerEndpoints;

/**
 * 계좌 샤딩 U4 — 담당 슬롯을 사람이 적은 정적 설정({@code shard-routing.shards})이 아니라 Kafka
 * 컨슈머 그룹 배정으로 받는다.
 *
 * <h3>메커니즘 — 파티션을 슬롯으로만 쓴다</h3>
 * <p>조정용 토픽 {@link #ASSIGNMENT_TOPIC}(파티션 수 = slot-count)에는 메시지를 한 번도 안 보낸다 —
 * 파티션 번호를 "나눠 가질 슬롯 번호"로만 쓴다. 이 워커가 그룹 {@link #GROUP_ID}에 조인하면,
 * 브로커가 배정한 파티션 번호 집합이 그대로 이 워커가 담당하는 슬롯 번호 집합이다.</p>
 *
 * <h3>재시작해도 슬롯이 안 바뀐다(L1) — static membership</h3>
 * <p>{@code group.instance.id}(이 워커의 고정 신원)를 준 컨슈머로 조인한다({@link
 * com.flab.stocktradingengine.account.worker.config.AccountShardOwnershipConfig}가 배선). 정적
 * 멤버십에서는 {@link #close()}로 정상 종료해도 브로커에 LeaveGroup을 보내지 않는다 — 그래서
 * 크래시든 정상 종료든 구분 없이, 같은 신원이 {@code session.timeout.ms} 안에 돌아오면 같은
 * 슬롯을 그대로 돌려받고, 못 돌아오면(진짜로 없어짐) 그제서야 재배정된다. 대기 워커·옛 주인
 * 차단(fencing)은 이 트랙 범위가 아니다(L3).</p>
 *
 * <h3>빈 집합으로 시작한다</h3>
 * <p>기동 직후~최초 배정 사이엔 {@link #test}가 항상 false다 — 그 사이 들어온 주문은
 * {@code AccountEventHandler}의 NOT_OWNED 거부로 처리된다(조용히 버리지 않음, api가 재시도).</p>
 */
public final class KafkaShardAssignment implements IntPredicate {

    private static final Logger log = System.getLogger(KafkaShardAssignment.class.getName());

    public static final String ASSIGNMENT_TOPIC = "account-shard-assignment";
    public static final String GROUP_ID = "account-shard-owners";

    private final Consumer<String, String> consumer;
    private final Producer<Integer, String> mapProducer;
    private final Admin adminClient;
    private final int slotCount;
    private final WorkerEndpoints ownEndpoints;

    // 핫패스(test)가 매 주문마다 부른다 — Set<Integer>였을 때는 Integer 오토박싱이 캐시 범위
    // (-128~127)를 넘는 슬롯마다 새 객체를 만들었다(slotCount=256이면 절반이 매번 할당). 배열
    // 참조를 volatile로 통째로 갈아끼우면 박싱 없이 인덱스 조회 하나로 끝난다 — happens-before는
    // 그대로 volatile 필드가 보장한다.
    private volatile boolean[] ownedSlotFlags;
    private volatile boolean running;
    private Thread pollThread;

    // U6 — L1("배정은 기동할 때 정해지고 바뀌지 않는다")을 코드로 못 박는 안전망. 리밸런스
    // 콜백은 poll 스레드 하나에서만 불리므로 이 필드들은 그 스레드만 읽고 쓴다(동시성 보호 불필요).
    private boolean initialAssignmentReceived;
    private Set<Integer> initialSlots = Set.of();

    public KafkaShardAssignment(Consumer<String, String> consumer, Producer<Integer, String> mapProducer,
                                Admin adminClient, int slotCount, WorkerEndpoints ownEndpoints) {
        this.consumer = consumer;
        this.mapProducer = mapProducer;
        this.adminClient = adminClient;
        this.slotCount = slotCount;
        this.ownEndpoints = ownEndpoints;
        this.ownedSlotFlags = new boolean[slotCount];
    }

    @Override
    public boolean test(int slot) {
        boolean[] flags = ownedSlotFlags;
        return slot >= 0 && slot < flags.length && flags[slot];
    }

    /** 지금 이 워커가 담당하는 슬롯 집합(로그·시딩 안내용) — 필터링에는 {@link #test}를 쓴다. */
    public Set<Integer> currentSlotsSnapshot() {
        boolean[] flags = ownedSlotFlags;
        return IntStream.range(0, flags.length).filter(slot -> flags[slot]).boxed()
            .collect(Collectors.toUnmodifiableSet());
    }

    public void start() {
        ensureTopicPartitionCount();
        ensureMapTopicExists();
        running = true;
        pollThread = new Thread(this::pollLoop, "account-shard-assignment-poll");
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
        consumer.subscribe(List.of(ASSIGNMENT_TOPIC), rebalanceListener());
        try {
            while (running) {
                consumer.poll(Duration.ofMillis(500));
            }
        } catch (WakeupException e) {
            // close()가 의도적으로 poll을 깨운 것 — 정상 종료 경로.
        }
    }

    /**
     * 배정을 잃으면(회수) {@code account-shard-map}에 그 슬롯의 값을 tombstone(null)으로 지운다.
     * L1에서는 살아 있는 다른 워커가 안 가져가므로, 지우지 않으면 그 슬롯이 죽은 나(정확히는 더
     * 못 받는 나)를 계속 가리켜 api·매칭이 이미 못 받는 목적지로 계속 보낸다 — 요청이 성공한
     * 것처럼 Aeron까지는 가지만 이 워커의 isOwned()가 거부해 조용히 어긋난다. 지우면 그 순간부터
     * "목적지 없음"(503/재시도)이 되고, 실제로 재배정이 일어나면 새 주인이 자기 endpoint로 다시
     * 채운다 — 그때까지의 공백은 L1이 이미 받아들인 대가(그 계좌만 몇 초 503)와 같은 종류다.
     *
     * <p>revoke를 동기로(즉시 {@code get()}) 보내는 이유: eager 리밸런스 프로토콜은 참가자 전원의
     * revoke가 끝나야 다음 참가자의 assign이 열린다 — 그 전에 이 tombstone이 브로커에 실제로
     * 써져 있어야, 새 주인의 assign 메시지가 먼저 가고 tombstone이 나중에 덮어써 새 값을 지우는
     * 순서 역전을 막는다.</p>
     */
    private ConsumerRebalanceListener rebalanceListener() {
        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                boolean[] updated = ownedSlotFlags.clone();
                partitions.forEach(p -> updated[p.partition()] = false);
                ownedSlotFlags = updated;
                publishMapEntries(partitions, null);
                log.log(Level.WARNING, "[계좌] 담당 슬롯 회수: " + slotNumbers(partitions) + " 남은 담당=" + currentSlotsSnapshot().size() + "개");
            }

            /**
             * U6 — 이 워커가 살아 있는 동안 처음 받은 배정만 진짜로 받아들인다. 그 뒤 리밸런스로
             * (Kafka 설정이 뚫려서든, 다른 워커가 죽어 session.timeout 뒤 재배정됐든) 처음 배정에
             * 없던 슬롯이 섞여 들어오면 거부한다 — 이 워커는 그 슬롯 계좌들의 상태(잔고·예약)를
             * 메모리에 가진 적이 없으므로, 받아들이면 아무 상태 없이 주문을 처리하는 것과 같다.
             * 슬롯이 줄어드는 것(회수)은 그대로 받아들인다 — 내가 죽는 중일 수 있어 막을 이유가 없다.
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                List<TopicPartition> accepted;
                List<TopicPartition> rejected;
                if (!initialAssignmentReceived) {
                    accepted = new ArrayList<>(partitions);
                    rejected = List.of();
                    initialSlots = partitions.stream().map(TopicPartition::partition).collect(Collectors.toUnmodifiableSet());
                    initialAssignmentReceived = true;
                } else {
                    accepted = new ArrayList<>();
                    rejected = new ArrayList<>();
                    for (TopicPartition partition : partitions) {
                        (initialSlots.contains(partition.partition()) ? accepted : rejected).add(partition);
                    }
                }

                boolean[] updated = ownedSlotFlags.clone();
                accepted.forEach(p -> updated[p.partition()] = true);
                ownedSlotFlags = updated;
                if (!accepted.isEmpty()) {
                    publishMapEntries(accepted, ownEndpoints.encode());
                }
                if (!rejected.isEmpty()) {
                    // 거부한 슬롯은 account-shard-map에 아무것도 발행하지 않는다 — 이 워커가 담당인
                    // 척하면 api·매칭이 계속 여기로 보내고 매번 NOT_OWNED로 어긋난다. "목적지 없음"
                    // (503/재시도) 상태로 두는 편이 낫다.
                    log.log(Level.WARNING, "[계좌] 최초 배정에 없던 슬롯 거부(상태 없음, L1 안전망): " + slotNumbers(rejected));
                }
                log.log(Level.INFO, "[계좌] 담당 슬롯 배정: 신규=" + slotNumbers(accepted) + " 전체 담당=" + currentSlotsSnapshot().size() + "개");
            }
        };
    }

    private void publishMapEntries(Collection<TopicPartition> partitions, String value) {
        for (TopicPartition partition : partitions) {
            try {
                mapProducer.send(new ProducerRecord<>(AssignmentDestinationResolver.MAP_TOPIC, partition.partition(), value)).get();
            } catch (ExecutionException e) {
                throw new IllegalStateException(
                    "account-shard-map 발행 실패: slot=" + partition.partition() + " value=" + value, e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("account-shard-map 발행 중 인터럽트됨", e);
            }
        }
    }

    private List<Integer> slotNumbers(Collection<TopicPartition> partitions) {
        return partitions.stream().map(TopicPartition::partition).toList();
    }

    /**
     * 토픽이 없으면 slot-count와 같은 파티션 수로 만든다. 이미 있는데 파티션 수가 slot-count와
     * 다르면 기동을 막는다(fail-fast) — 조정 토픽과 실제 슬롯 수가 어긋난 채로 뜨면 일부 슬롯이
     * 영원히 배정되지 않는다.
     */
    private void ensureTopicPartitionCount() {
        try {
            Map<String, TopicDescription> described = adminClient.describeTopics(List.of(ASSIGNMENT_TOPIC)).allTopicNames().get();
            int actual = described.get(ASSIGNMENT_TOPIC).partitions().size();
            if (actual != slotCount) {
                throw new IllegalStateException(
                    "조정 토픽 " + ASSIGNMENT_TOPIC + "의 파티션 수(" + actual + ")가 shard-routing.slot-count(" + slotCount
                        + ")와 다릅니다 — 설정이 어긋났습니다");
            }
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                createTopic();
                return;
            }
            throw new IllegalStateException("조정 토픽 조회 실패: " + ASSIGNMENT_TOPIC, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("조정 토픽 조회 중 인터럽트됨", e);
        }
    }

    private void createTopic() {
        try {
            adminClient.createTopics(List.of(new NewTopic(ASSIGNMENT_TOPIC, slotCount, (short) 1))).all().get();
            log.log(Level.INFO, "[계좌] 조정 토픽 생성: " + ASSIGNMENT_TOPIC + " partitions=" + slotCount);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new IllegalStateException("조정 토픽 생성 실패: " + ASSIGNMENT_TOPIC, e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("조정 토픽 생성 중 인터럽트됨", e);
        }
    }

    /**
     * {@code account-shard-map}은 최신 값만 있으면 되는 KV 저장소라 compacted로 만든다 — delete
     * (기본값)면 tombstone이 retention 기간 뒤 사라져도 되지만, compact면 tombstone도 "그 키를
     * 지워라"는 뜻으로 영구히(정확히는 다음 압착 전까지) 남는다. 파티션 수는 1로 둔다 — 키(슬롯)당
     * 최대 256건뿐이라 병렬성이 필요 없고, 파티션이 하나면 전체 메시지가 하나의 로그로 정렬돼
     * revoke→assign 순서를 더 단순하게 보장한다(키 해시로도 같은 슬롯은 항상 같은 파티션에 가므로
     * 여러 파티션이어도 키별 순서는 지켜지지만, 굳이 그 보장에 기댈 이유가 없다).
     */
    private void ensureMapTopicExists() {
        try {
            adminClient.describeTopics(List.of(AssignmentDestinationResolver.MAP_TOPIC)).allTopicNames().get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                createMapTopic();
                return;
            }
            throw new IllegalStateException("account-shard-map 조회 실패", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("account-shard-map 조회 중 인터럽트됨", e);
        }
    }

    private void createMapTopic() {
        try {
            NewTopic topic = new NewTopic(AssignmentDestinationResolver.MAP_TOPIC, 1, (short) 1)
                .configs(Map.of("cleanup.policy", "compact"));
            adminClient.createTopics(List.of(topic)).all().get();
            log.log(Level.INFO, "[계좌] account-shard-map 토픽 생성(compacted)");
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
