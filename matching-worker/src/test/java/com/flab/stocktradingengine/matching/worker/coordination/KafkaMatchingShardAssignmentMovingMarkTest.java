package com.flab.stocktradingengine.matching.worker.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.MatchingSlotTarget;
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventRequests;

/**
 * 회수 콜백이 맵 토픽에 올리는 값. 지우는 레코드(tombstone) 대신 이전 담당 자신의 주소에 이동 중 표시를 붙인 값
 * ({@code 주소;moving})을 올려, 새 담당이 이전 담당의 주소를 맵에서 알 수 있고 계좌 shard는 그 종목 slot의 주문을
 * 보내지 않고 보관하게 한다(10-2절 수정본, 11-13절 1번).
 */
class KafkaMatchingShardAssignmentMovingMarkTest {

    private static final String OWN_ENDPOINT = "aeron:udp?endpoint=localhost:20020";

    private final MockProducer<Integer, String> mapProducer =
        new MockProducer<>(true, new IntegerSerializer(), new StringSerializer());

    @Test
    void 회수_콜백은_맵에_이전_담당_주소에_moving을_붙인_값을_올린다() {
        KafkaMatchingShardAssignment assignment = new KafkaMatchingShardAssignment(mock(Consumer.class), mapProducer,
            mock(Admin.class), 64, OWN_ENDPOINT, "assignment-topic", "map-topic", new SlotEventRequests());
        ConsumerRebalanceListener listener = assignment.rebalanceListener();

        listener.onPartitionsRevoked(List.of(new TopicPartition("assignment-topic", 3), new TopicPartition("assignment-topic", 5)));

        List<ProducerRecord<Integer, String>> sent = mapProducer.history();
        assertThat(sent).extracting(ProducerRecord::key).containsExactly(3, 5);
        assertThat(sent).extracting(ProducerRecord::value)
            .containsOnly(new MatchingSlotTarget(OWN_ENDPOINT, true).encode())
            .doesNotContainNull();
    }
}
