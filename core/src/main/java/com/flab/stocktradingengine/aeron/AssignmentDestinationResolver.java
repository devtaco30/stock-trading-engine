package com.flab.stocktradingengine.aeron;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

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
 * <p>{@link #endpointFor}는 api의 매 주문·매칭의 매 체결마다 불린다. {@code KafkaShardAssignment}와
 * 같은 이유로 {@code Map<Integer, String>}(오토박싱) 대신 슬롯 번호로 바로 인덱싱하는
 * {@code volatile String[]}을 쓴다 — 갱신할 때 배열을 통째로 복사해 갈아끼우고, 읽는 쪽은
 * volatile 참조 한 번 읽고 인덱스 조회 하나로 끝난다.</p>
 */
public final class AssignmentDestinationResolver implements AccountDestinationResolver {

    private static final Logger log = System.getLogger(AssignmentDestinationResolver.class.getName());

    public static final String MAP_TOPIC = "account-shard-map";

    private final Consumer<Integer, String> consumer;
    private final ShardRoutingTable shardRoutingTable;

    private volatile String[] slotToEndpoint;
    private volatile boolean running;
    private Thread pollThread;

    public AssignmentDestinationResolver(Consumer<Integer, String> consumer, ShardRoutingTable shardRoutingTable, int slotCount) {
        this.consumer = consumer;
        this.shardRoutingTable = shardRoutingTable;
        this.slotToEndpoint = new String[slotCount];
    }

    @Override
    public Optional<String> endpointFor(long accountId) {
        int slot = shardRoutingTable.slotFor(accountId);
        String[] snapshot = slotToEndpoint;
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
        running = true;
        pollThread = new Thread(this::pollLoop, "account-shard-map-poll");
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
    }

    private void pollLoop() {
        try {
            while (running) {
                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(500));
                if (!records.isEmpty()) {
                    applyRecords(records);
                }
            }
        } catch (WakeupException e) {
            // close()가 의도적으로 poll을 깨운 것 — 정상 종료 경로.
        }
    }

    private void applyRecords(ConsumerRecords<Integer, String> records) {
        String[] updated = slotToEndpoint.clone();
        for (ConsumerRecord<Integer, String> record : records) {
            int slot = record.key();
            if (slot < 0 || slot >= updated.length) {
                log.log(Level.WARNING, "[목적지] " + MAP_TOPIC + "에서 범위 밖 슬롯을 받음: slot=" + slot + " slotCount=" + updated.length);
                continue;
            }
            // value가 null이면 tombstone(그 워커가 슬롯을 회수당함) — 목적지 없음으로 되돌린다.
            updated[slot] = record.value();
        }
        slotToEndpoint = updated;
        log.log(Level.INFO, "[목적지] " + MAP_TOPIC + " " + records.count() + "건 반영");
    }
}
