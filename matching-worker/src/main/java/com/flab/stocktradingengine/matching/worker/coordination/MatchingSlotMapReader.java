package com.flab.stocktradingengine.matching.worker.coordination;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import com.flab.stocktradingengine.aeron.MatchingSlotTarget;

/**
 * 매칭 shard가 맵 토픽 {@code matching-shard-map}(종목 slot → 그 종목 slot을 담당하는 pod의 주소, 이동 중이면 {@code 주소;moving})을
 * 읽는다. 호가창 이동을 받는 쪽(Z)이 배정 콜백에서 그 종목 slot의 이전 담당(X)이 맵에 남긴 항목을 찾는 데 쓴다(11-13절 1·2번).
 * 계좌 shard의 {@code MatchingDestinationResolver}와 같은 방식이다: 컨슈머 그룹 없이 {@code assign}으로 모든 파티션을 직접 맡아
 * 처음부터 계속 읽고, 종목 slot별 최신 값을 {@code volatile} 배열에 둔다. 갱신은 poll thread 하나만 하고, 한 poll에서 받은 변경은
 * 배열 교체 한 번으로 함께 보인다.
 */
public final class MatchingSlotMapReader {

    private static final Logger log = System.getLogger(MatchingSlotMapReader.class.getName());

    private final Consumer<Integer, String> consumer;
    private final String mapTopic;

    private volatile MatchingSlotTarget[] slotToTarget;
    private volatile boolean running;
    private Thread pollThread;
    private final CountDownLatch initialCatchUpLatch = new CountDownLatch(1);
    // 읽는 thread가 예외로 끝났으면 그 예외. 초기 읽기 대기가 이것을 보고 실패로 끝난다.
    private volatile Throwable failure;
    // 파티션마다 읽어 반영한 다음 위치. 읽는 thread가 poll마다 갱신하고 다른 thread가 따라왔는지 볼 때 읽는다.
    private volatile java.util.concurrent.atomic.AtomicLongArray consumedOffsets = new java.util.concurrent.atomic.AtomicLongArray(1);
    private final List<MapChangeListener> listeners = new CopyOnWriteArrayList<>();

    public MatchingSlotMapReader(Consumer<Integer, String> consumer, int stockSlotCount, String mapTopic) {
        this.consumer = consumer;
        this.mapTopic = mapTopic;
        this.slotToTarget = new MatchingSlotTarget[stockSlotCount];
    }

    /** 맵 항목이 바뀔 때 불리는 관찰자. 읽는 thread에서 불리므로 오래 걸리는 일을 하지 않는다. */
    @FunctionalInterface
    public interface MapChangeListener {

        void onChange(int stockSlot, Optional<MatchingSlotTarget> target);
    }

    /** 그 파티션에서 읽어 반영한 다음 위치(시작 전에는 0). */
    public long consumedOffset(int partition) {
        java.util.concurrent.atomic.AtomicLongArray offsets = consumedOffsets;
        return partition >= 0 && partition < offsets.length() ? offsets.get(partition) : 0L;
    }

    public void addListener(MapChangeListener listener) {
        listeners.add(listener);
    }

    /** 이 종목 slot의 맵 항목. 항목이 없거나 형식을 읽지 못했으면 빈 값이다. */
    public Optional<MatchingSlotTarget> targetFor(int stockSlot) {
        MatchingSlotTarget[] snapshot = slotToTarget;
        if (stockSlot < 0 || stockSlot >= snapshot.length) {
            return Optional.empty();
        }
        return Optional.ofNullable(snapshot[stockSlot]);
    }

    // 토픽을 만든 직후에는 파티션 정보가 아직 비어 있을 수 있다. 나타날 때까지 이 시간 동안 다시 물어본다.
    private volatile long partitionWaitMillis = 10_000L;

    /** 패키지 가시성 — 시험이 기다리는 시간을 줄인다. */
    void setPartitionWaitMillis(long millis) {
        this.partitionWaitMillis = millis;
    }

    private List<PartitionInfo> awaitPartitions() {
        long deadline = System.nanoTime() + partitionWaitMillis * 1_000_000L;
        while (true) {
            List<PartitionInfo> infos = consumer.partitionsFor(mapTopic);
            if (infos != null && !infos.isEmpty()) {
                return infos;
            }
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException(mapTopic + "의 파티션 정보가 " + partitionWaitMillis + "ms 안에 나타나지 않았습니다");
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(mapTopic + " 파티션 정보를 기다리다 인터럽트됨", e);
            }
        }
    }

    public void start() {
        List<PartitionInfo> partitionInfos = awaitPartitions();
        List<TopicPartition> partitions = partitionInfos.stream()
            .map(info -> new TopicPartition(mapTopic, info.partition())).toList();
        consumedOffsets = new java.util.concurrent.atomic.AtomicLongArray(partitions.size());
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
        running = true;
        pollThread = new Thread(() -> pollLoop(partitions, endOffsets), "matching-shard-map-reader");
        pollThread.setDaemon(true);
        pollThread.start();
    }

    /** 지금까지 쌓인 기록을 모두 읽을 때까지 기다린다. 토픽이 비어 있으면 바로 돌아온다. */
    public void awaitInitialCatchUp(Duration timeout) {
        try {
            if (!initialCatchUpLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException(mapTopic + " 초기 읽기가 " + timeout + " 안에 끝나지 않았습니다");
            }
            if (failure != null) {
                throw new IllegalStateException(mapTopic + " 읽는 thread가 비정상 종료했습니다", failure);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(mapTopic + " 초기 읽기 대기 중 인터럽트됨", e);
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
                for (TopicPartition partition : partitions) {
                    consumedOffsets.set(partition.partition(), consumer.position(partition));
                }
                if (!caughtUp && isCaughtUp(partitions, endOffsets)) {
                    caughtUp = true;
                    initialCatchUpLatch.countDown();
                }
            }
        } catch (WakeupException e) {
            // close()가 poll을 깨운 것이다.
        } catch (RuntimeException e) {
            failure = e;
            log.log(Level.ERROR, "[맵] " + mapTopic + " 읽는 thread가 예외로 끝남: " + e);
        } finally {
            initialCatchUpLatch.countDown();
        }
    }

    private boolean isCaughtUp(List<TopicPartition> partitions, Map<TopicPartition, Long> endOffsets) {
        for (TopicPartition partition : partitions) {
            if (consumer.position(partition) < endOffsets.get(partition)) {
                return false;
            }
        }
        return true;
    }

    /** 패키지 가시성 — 단위 시험이 레코드를 직접 넣는다. poll thread에서만 불러야 한다. */
    void applyRecords(ConsumerRecords<Integer, String> records) {
        MatchingSlotTarget[] updated = slotToTarget.clone();
        // 반영한 항목마다 그 시점의 값을 모아 두었다가 배열을 교체한 뒤 관찰자에게 알린다.
        List<Map.Entry<Integer, Optional<MatchingSlotTarget>>> changes = new java.util.ArrayList<>();
        for (ConsumerRecord<Integer, String> record : records) {
            int slot = record.key();
            if (slot < 0 || slot >= updated.length) {
                log.log(Level.WARNING, "[맵] " + mapTopic + "에서 범위 밖 종목 slot을 받음: " + slot);
                continue;
            }
            if (record.value() == null) {
                updated[slot] = null;
            } else {
                try {
                    updated[slot] = MatchingSlotTarget.parse(record.value());
                } catch (IllegalArgumentException e) {
                    log.log(Level.WARNING, "[맵] " + mapTopic + "에서 읽을 수 없는 값: 종목 slot=" + slot + " 값=" + record.value());
                    updated[slot] = null;
                }
            }
            changes.add(Map.entry(slot, Optional.ofNullable(updated[slot])));
        }
        slotToTarget = updated;
        for (Map.Entry<Integer, Optional<MatchingSlotTarget>> change : changes) {
            for (MapChangeListener listener : listeners) {
                try {
                    listener.onChange(change.getKey(), change.getValue());
                } catch (RuntimeException e) {
                    log.log(Level.ERROR, "[맵] 관찰자 처리 중 예외(읽기는 계속함): 종목 slot=" + change.getKey() + " 원인=" + e);
                }
            }
        }
    }
}
