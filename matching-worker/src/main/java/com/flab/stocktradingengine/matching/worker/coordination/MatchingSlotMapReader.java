package com.flab.stocktradingengine.matching.worker.coordination;

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

    public MatchingSlotMapReader(Consumer<Integer, String> consumer, int stockSlotCount, String mapTopic) {
        this.consumer = consumer;
        this.mapTopic = mapTopic;
        this.slotToTarget = new MatchingSlotTarget[stockSlotCount];
    }

    /** 이 종목 slot의 맵 항목. 항목이 없거나 형식을 읽지 못했으면 빈 값이다. */
    public Optional<MatchingSlotTarget> targetFor(int stockSlot) {
        MatchingSlotTarget[] snapshot = slotToTarget;
        if (stockSlot < 0 || stockSlot >= snapshot.length) {
            return Optional.empty();
        }
        return Optional.ofNullable(snapshot[stockSlot]);
    }

    public void start() {
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(mapTopic);
        List<TopicPartition> partitions = partitionInfos.stream()
            .map(info -> new TopicPartition(mapTopic, info.partition())).toList();
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
        for (ConsumerRecord<Integer, String> record : records) {
            int slot = record.key();
            if (slot < 0 || slot >= updated.length) {
                log.log(Level.WARNING, "[맵] " + mapTopic + "에서 범위 밖 종목 slot을 받음: " + slot);
                continue;
            }
            if (record.value() == null) {
                updated[slot] = null;
                continue;
            }
            try {
                updated[slot] = MatchingSlotTarget.parse(record.value());
            } catch (IllegalArgumentException e) {
                log.log(Level.WARNING, "[맵] " + mapTopic + "에서 읽을 수 없는 값: 종목 slot=" + slot + " 값=" + record.value());
                updated[slot] = null;
            }
        }
        slotToTarget = updated;
    }
}
