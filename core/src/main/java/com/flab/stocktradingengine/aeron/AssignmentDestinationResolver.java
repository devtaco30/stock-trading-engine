package com.flab.stocktradingengine.aeron;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

/**
 * 계좌 샤딩 U5 — {@link AccountDestinationResolver}의 둘째 구현(U1). 계좌 워커들이
 * {@code account-shard-map}(슬롯 번호 → 그 슬롯을 담당하는 워커의 Aeron endpoint, compacted)에
 * 발행한 배정을 구독해, api·매칭 워커가 "지금 이 슬롯은 누가 담당인가"를 실시간으로 안다.
 *
 * <h3>컨슈머 그룹이 아니라 전체 구독이다</h3>
 * <p>계좌 워커의 {@code KafkaShardAssignment}는 파티션(슬롯)을 나눠 갖는 것이 목적이라 컨슈머
 * 그룹이 맞지만, 이 클래스는 반대다 — api·매칭 워커 인스턴스마다 목적지 표 전체가 다 있어야
 * 라우팅할 수 있으므로 그룹 없이(subscribe가 아니라 {@code assign}) 모든 파티션을 직접 맡아
 * 처음부터({@code seekToBeginning}) 계속 읽는다.</p>
 *
 * <h3>핫패스 — 락 없는 배열 통째 교체</h3>
 * <p>{@link #orderEndpointFor}·{@link #fillEndpointFor}는 api의 매 주문·매칭의 매 체결마다 불린다. {@code KafkaShardAssignment}와
 * 같은 이유로 {@code Map<Integer, String>}(오토박싱) 대신 슬롯 번호로 바로 인덱싱하는
 * {@code volatile String[]}을 쓴다 — 갱신할 때 배열을 통째로 복사해 갈아끼우고, 읽는 쪽은
 * volatile 참조 한 번 읽고 인덱스 조회 하나로 끝난다.</p>
 */
public final class AssignmentDestinationResolver implements AccountDestinationResolver {

    private static final Logger log = System.getLogger(AssignmentDestinationResolver.class.getName());

    public static final String MAP_TOPIC = "account-shard-map";

    private final Consumer<Integer, String> consumer;
    private final SlotHasher slotHasher;

    private volatile WorkerEndpoints[] slotToEndpoints;
    private volatile boolean running;
    private Thread pollThread;
    private final CountDownLatch initialCatchUpLatch = new CountDownLatch(1);

    /**
     * 칸 개수는 {@link SlotHasher}가 들고 있는 값 하나만 쓴다 — 계산에 쓰는 칸 개수와 목적지를
     * 담아 두는 칸 개수가 따로 주어지면 서로 어긋날 수 있다.
     */
    public AssignmentDestinationResolver(Consumer<Integer, String> consumer, SlotHasher slotHasher) {
        this.consumer = consumer;
        this.slotHasher = slotHasher;
        this.slotToEndpoints = new WorkerEndpoints[slotHasher.slotCount()];
    }

    @Override
    public Optional<String> orderEndpointFor(long accountId) {
        return endpointsFor(accountId).map(WorkerEndpoints::orderEndpoint);
    }

    @Override
    public Optional<String> fillEndpointFor(long accountId) {
        return endpointsFor(accountId).map(WorkerEndpoints::fillEndpoint);
    }

    private Optional<WorkerEndpoints> endpointsFor(long accountId) {
        int slot = slotHasher.slotFor(accountId);
        WorkerEndpoints[] snapshot = slotToEndpoints;
        if (slot < 0 || slot >= snapshot.length) {
            return Optional.empty();
        }
        return Optional.ofNullable(snapshot[slot]);
    }

    public void start() {
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(MAP_TOPIC);
        List<TopicPartition> partitions = partitionInfos.stream()
            .map(info -> new TopicPartition(MAP_TOPIC, info.partition()))
            .toList();
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
        // 지금까지 쌓인 기록을 다 읽었다고 판정할 기준선. 이 값 이후에 도착하는 레코드는 "아직
        // 못 읽은 과거"가 아니라 "그때부터 새로 일어난 배정 변경"이라 초기 읽기 판정에 안 쓴다.
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
        running = true;
        pollThread = new Thread(() -> pollLoop(partitions, endOffsets), "account-shard-map-poll");
        pollThread.setDaemon(true);
        pollThread.start();
    }

    /**
     * 지금까지 {@code account-shard-map}에 쌓인 기록을 전부 읽을 때까지 호출 스레드를 기다리게
     * 한다(계좌 샤딩 U6) — api·매칭 워커가 "아직 아무 배정도 모르는 채로" 트래픽을 받기 시작하는
     * 것을 막는 용도다. 매칭에서는 {@code MatchingOrderReceiverLifecycle}이 이걸로 주문 인테이크
     * 구독을 늦춘다. 토픽이 비어 있으면(아직 아무도 배정을 못 받음) 곧바로 반환한다 — 데이터가
     * "언젠가 올 때까지" 무한히 기다리지 않는다.
     *
     * @throws IllegalStateException timeout 안에 못 끝나면(브로커 문제 등)
     */
    public void awaitInitialCatchUp(Duration timeout) {
        try {
            if (!initialCatchUpLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException(
                    MAP_TOPIC + " 초기 읽기가 " + timeout + " 안에 끝나지 않았습니다");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(MAP_TOPIC + " 초기 읽기 대기 중 인터럽트됨", e);
        }
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
    }

    private void pollLoop(List<TopicPartition> partitions, Map<TopicPartition, Long> endOffsets) {
        boolean caughtUp = false;
        try {
            while (running) {
                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(500));
                if (!records.isEmpty()) {
                    applyRecords(records);
                }
                if (!caughtUp && isCaughtUp(partitions, endOffsets)) {
                    caughtUp = true;
                    initialCatchUpLatch.countDown();
                }
            }
        } catch (WakeupException e) {
            // close()가 의도적으로 poll을 깨운 것 — 정상 종료 경로.
        } finally {
            // running이 false가 돼 루프를 빠져나가는 정상 종료 경로에서도(close() 등) 대기 중인
            // awaitInitialCatchUp 호출자가 영원히 안 깨는 일이 없게 한다.
            initialCatchUpLatch.countDown();
        }
    }

    /** consumer.position()은 이 poll 스레드에서만 부른다 — KafkaConsumer는 여러 스레드에서 동시에 쓸 수 없다. */
    private boolean isCaughtUp(List<TopicPartition> partitions, Map<TopicPartition, Long> endOffsets) {
        for (TopicPartition partition : partitions) {
            if (consumer.position(partition) < endOffsets.get(partition)) {
                return false;
            }
        }
        return true;
    }

    private void applyRecords(ConsumerRecords<Integer, String> records) {
        WorkerEndpoints[] updated = slotToEndpoints.clone();
        for (ConsumerRecord<Integer, String> record : records) {
            int slot = record.key();
            if (slot < 0 || slot >= updated.length) {
                log.log(Level.WARNING, "[목적지] " + MAP_TOPIC + "에서 범위 밖 슬롯을 받음: slot=" + slot + " slotCount=" + updated.length);
                continue;
            }
            // value가 null이면 tombstone(그 워커가 슬롯을 회수당함) — 목적지 없음으로 되돌린다.
            if (record.value() == null) {
                updated[slot] = null;
                continue;
            }
            try {
                updated[slot] = WorkerEndpoints.parse(record.value());
            } catch (IllegalArgumentException e) {
                // 형식이 깨진 값 하나 때문에 나머지 배정까지 잃지 않는다. 그 슬롯만 목적지 없음으로
                // 두면 보내는 쪽이 503으로 되돌려 재시도한다.
                log.log(Level.WARNING, "[목적지] " + MAP_TOPIC + "에서 읽을 수 없는 값: slot=" + slot
                    + " value=" + record.value() + " — 이 슬롯은 목적지 없음으로 둔다");
                updated[slot] = null;
            }
        }
        slotToEndpoints = updated;
        log.log(Level.INFO, "[목적지] " + MAP_TOPIC + " " + records.count() + "건 반영");
    }
}
