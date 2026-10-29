package com.flab.stocktradingengine.account.worker.u10;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.account.worker.coordination.MatchingDestinationResolver;
import com.flab.stocktradingengine.aeron.StockSlotHasher;

/** U10 확인 스크립트가 쓰는 도구의 단위 시험. 브로커 없이 MockConsumer로 맵 토픽 읽기를 흉내 낸다. */
class U10ToolTest {

    private static final String MAP_TOPIC = MatchingDestinationResolver.MAP_TOPIC;

    private static MockConsumer<Integer, String> consumerWithPartitions(int partitionCount, long endOffset) {
        MockConsumer<Integer, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        List<PartitionInfo> infos = new ArrayList<>();
        Map<TopicPartition, Long> begin = new HashMap<>();
        Map<TopicPartition, Long> end = new HashMap<>();
        for (int partition = 0; partition < partitionCount; partition++) {
            infos.add(new PartitionInfo(MAP_TOPIC, partition, null, null, null));
            begin.put(new TopicPartition(MAP_TOPIC, partition), 0L);
            end.put(new TopicPartition(MAP_TOPIC, partition), endOffset);
        }
        consumer.updatePartitions(MAP_TOPIC, infos);
        consumer.updateBeginningOffsets(begin);
        consumer.updateEndOffsets(end);
        return consumer;
    }

    private static String run(java.util.function.Consumer<PrintStream> action) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(bytes, true, StandardCharsets.UTF_8);
        action.accept(stream);
        return bytes.toString(StandardCharsets.UTF_8);
    }

    @Test
    void 끝까지_읽지_못하면_WARN_PARTIAL_MAP_READ를_표준_오류_스트림에_남기고_읽은_만큼만_돌려준다() {
        MockConsumer<Integer, String> consumer = consumerWithPartitions(1, 5L);
        consumer.schedulePollTask(() -> {
            consumer.addRecord(new ConsumerRecord<>(MAP_TOPIC, 0, 0L, 0, "aeron:udp?endpoint=localhost:20020"));
            consumer.addRecord(new ConsumerRecord<>(MAP_TOPIC, 0, 1L, 0, "aeron:udp?endpoint=localhost:20020;moving"));
        });
        List<U10Tool.MapRecord> records = new ArrayList<>();
        String err = run(stream -> records.addAll(U10Tool.readMapTopic(consumer, 300L, stream)));
        assertEquals(2, records.size());
        assertTrue(err.contains("WARN_PARTIAL_MAP_READ"), err);
    }

    @Test
    void 끝까지_읽으면_경고를_남기지_않는다() {
        MockConsumer<Integer, String> consumer = consumerWithPartitions(1, 2L);
        consumer.schedulePollTask(() -> {
            consumer.addRecord(new ConsumerRecord<>(MAP_TOPIC, 0, 0L, 0, "aeron:udp?endpoint=localhost:20020"));
            consumer.addRecord(new ConsumerRecord<>(MAP_TOPIC, 0, 1L, 0, "aeron:udp?endpoint=localhost:20021"));
        });
        List<U10Tool.MapRecord> records = new ArrayList<>();
        String err = run(stream -> records.addAll(U10Tool.readMapTopic(consumer, 2_000L, stream)));
        assertEquals(2, records.size());
        assertFalse(err.contains("WARN_PARTIAL_MAP_READ"), err);
    }

    @Test
    void slot_of는_종목_코드마다_StockSlotHasher의_slot을_한_줄씩_돌려준다() {
        StockSlotHasher hasher = new StockSlotHasher(64);
        List<String> lines = U10Tool.slotLines(64, "000001", "000002", "000003");
        assertEquals(3, lines.size());
        assertEquals("SLOT 000001=" + hasher.slotFor("000001"), lines.get(0));
        assertEquals("SLOT 000003=" + hasher.slotFor("000003"), lines.get(2));
    }

    @Test
    void send_bulk_인자에서_표지_매수_옵션과_종목_코드를_가른다() {
        U10Tool.BulkTargets targets = U10Tool.parseBulkTargets(new String[] {
            "send-bulk", "ch", "90002", "SELL", "20000", "200", "p", "000001", "--sentinel-buy=90001:20000", "000002"}, 7);
        assertEquals(List.of("000001", "000002"), targets.stockCodes());
        assertEquals(90001L, targets.sentinelAccountId());
        assertEquals(20000L, targets.sentinelPrice());
    }

    @Test
    void send_bulk_인자에_표지_옵션이_없으면_표지를_보내지_않는다() {
        U10Tool.BulkTargets targets = U10Tool.parseBulkTargets(new String[] {
            "send-bulk", "ch", "90002", "SELL", "20000", "200", "p", "000001"}, 7);
        assertEquals(List.of("000001"), targets.stockCodes());
        assertEquals(0L, targets.sentinelAccountId());
    }
}
