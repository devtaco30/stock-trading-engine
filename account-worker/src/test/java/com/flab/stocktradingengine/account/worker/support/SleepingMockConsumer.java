package com.flab.stocktradingengine.account.worker.support;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/**
 * 브로커 없이 맵 토픽을 읽는 컨슈머 대역. {@link MockConsumer#poll}은 기다리지 않고 바로 돌아와 poll
 * 스레드가 CPU를 태우므로 짧게 쉰다. 토픽의 파티션을 미리 만들고 시작 오프셋·끝 오프셋을 0으로 둔다.
 */
public class SleepingMockConsumer extends MockConsumer<Integer, String> {

    public SleepingMockConsumer(String topic, int partitionCount) {
        super(OffsetResetStrategy.EARLIEST);
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        Map<TopicPartition, Long> zeroOffsets = new HashMap<>();
        for (int partition = 0; partition < partitionCount; partition++) {
            partitionInfos.add(new PartitionInfo(topic, partition, null, null, null));
            zeroOffsets.put(new TopicPartition(topic, partition), 0L);
        }
        updatePartitions(topic, partitionInfos);
        updateBeginningOffsets(zeroOffsets);
        updateEndOffsets(zeroOffsets);
    }

    @Override
    public synchronized ConsumerRecords<Integer, String> poll(Duration timeout) {
        ConsumerRecords<Integer, String> records = super.poll(timeout);
        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return records;
    }
}
