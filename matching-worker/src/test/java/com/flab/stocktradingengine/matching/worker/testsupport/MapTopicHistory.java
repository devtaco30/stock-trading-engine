package com.flab.stocktradingengine.matching.worker.testsupport;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/** 통합 시험이 맵 토픽을 별도 consumer로 읽는 도구. 시험 대상 pod의 맵 읽기와 무관하게 토픽에 쌓인 기록 전부를 순서대로 본다. */
public final class MapTopicHistory {

    private MapTopicHistory() {
    }

    /** 맵 토픽(파티션 0)을 처음부터 지금의 끝 위치까지 읽어 종목 slot별로 쌓인 값을 순서대로 모은다(tombstone은 null). */
    public static Map<Integer, List<String>> read(String bootstrap, String mapTopic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        Map<Integer, List<String>> history = new HashMap<>();
        TopicPartition partition = new TopicPartition(mapTopic, 0);
        try (KafkaConsumer<byte[], String> reader = new KafkaConsumer<>(props, new ByteArrayDeserializer(), new StringDeserializer())) {
            reader.assign(List.of(partition));
            reader.seekToBeginning(List.of(partition));
            long endOffset = reader.endOffsets(List.of(partition)).get(partition);
            while (reader.position(partition) < endOffset) {
                ConsumerRecords<byte[], String> records = reader.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], String> record : records) {
                    int slot = java.nio.ByteBuffer.wrap(record.key()).getInt();
                    history.computeIfAbsent(slot, key -> new ArrayList<>()).add(record.value());
                }
            }
        }
        return history;
    }
}
