package com.flab.stocktradingengine.matching.worker.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.MatchingSlotTarget;

/** 맵 토픽의 값을 종목 slot별 {@link MatchingSlotTarget}으로 읽는다(11-13절 2번). */
class MatchingSlotMapReaderTest {

    private static final String TOPIC = "matching-shard-map";
    private static final String X = "aeron:udp?endpoint=localhost:20020";

    private final MatchingSlotMapReader reader = new MatchingSlotMapReader(mock(Consumer.class), 64, TOPIC);

    @Test
    void 평상시_값과_이동_중_값을_읽는다() {
        reader.applyRecords(records(record(3, X), record(5, X + ";moving")));

        assertThat(reader.targetFor(3)).contains(new MatchingSlotTarget(X, false));
        assertThat(reader.targetFor(5)).contains(new MatchingSlotTarget(X, true));
        assertThat(reader.targetFor(4)).isEmpty();
    }

    @Test
    void 같은_종목_slot의_나중_값이_앞선_값을_대체한다() {
        reader.applyRecords(records(record(5, X), record(5, X + ";moving")));

        assertThat(reader.targetFor(5)).contains(new MatchingSlotTarget(X, true));
    }

    @Test
    void 값이_null이면_항목이_없다() {
        reader.applyRecords(records(record(5, X)));
        reader.applyRecords(records(record(5, null)));

        assertThat(reader.targetFor(5)).isEmpty();
    }

    @Test
    void 읽을_수_없는_값과_범위_밖_종목_slot은_그_항목만_건너뛴다() {
        reader.applyRecords(records(record(3, X), record(5, "  "), record(99, X)));

        assertThat(reader.targetFor(3)).contains(new MatchingSlotTarget(X, false));
        assertThat(reader.targetFor(5)).isEmpty();
        assertThat(reader.targetFor(99)).isEmpty();
    }

    @SafeVarargs
    private static ConsumerRecords<Integer, String> records(ConsumerRecord<Integer, String>... records) {
        return new ConsumerRecords<>(Map.of(new TopicPartition(TOPIC, 0), List.of(records)));
    }

    private static ConsumerRecord<Integer, String> record(int slot, String value) {
        return new ConsumerRecord<>(TOPIC, 0, 0L, slot, value);
    }
}
