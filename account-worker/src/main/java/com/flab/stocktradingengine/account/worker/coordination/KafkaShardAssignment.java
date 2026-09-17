package com.flab.stocktradingengine.account.worker.coordination;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.WakeupException;

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
    private final Admin adminClient;
    private final int slotCount;

    // 핫패스(test)가 매 주문마다 부른다 — Set<Integer>였을 때는 Integer 오토박싱이 캐시 범위
    // (-128~127)를 넘는 슬롯마다 새 객체를 만들었다(slotCount=256이면 절반이 매번 할당). 배열
    // 참조를 volatile로 통째로 갈아끼우면 박싱 없이 인덱스 조회 하나로 끝난다 — happens-before는
    // 그대로 volatile 필드가 보장한다.
    private volatile boolean[] ownedSlotFlags;
    private volatile boolean running;
    private Thread pollThread;

    public KafkaShardAssignment(Consumer<String, String> consumer, Admin adminClient, int slotCount) {
        this.consumer = consumer;
        this.adminClient = adminClient;
        this.slotCount = slotCount;
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

    private ConsumerRebalanceListener rebalanceListener() {
        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                boolean[] updated = ownedSlotFlags.clone();
                partitions.forEach(p -> updated[p.partition()] = false);
                ownedSlotFlags = updated;
                log.log(Level.WARNING, "[계좌] 담당 슬롯 회수: " + slotNumbers(partitions) + " 남은 담당=" + currentSlotsSnapshot().size() + "개");
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                boolean[] updated = ownedSlotFlags.clone();
                partitions.forEach(p -> updated[p.partition()] = true);
                ownedSlotFlags = updated;
                log.log(Level.INFO, "[계좌] 담당 슬롯 배정: 신규=" + slotNumbers(partitions) + " 전체 담당=" + currentSlotsSnapshot().size() + "개");
            }
        };
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
}
