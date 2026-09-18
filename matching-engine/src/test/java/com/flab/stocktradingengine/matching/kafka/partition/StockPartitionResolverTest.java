package com.flab.stocktradingengine.matching.kafka.partition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

@ExtendWith(MockitoExtension.class)
@DisplayName("StockPartitionResolver 단위 테스트")
class StockPartitionResolverTest {

    /**
     * 발행 쪽 계산의 기준값. kafka-clients 3.8.1 의 KafkaProducer#partition 은 키가 있고
     * partitioner.class 를 지정하지 않으면 이 메서드를 호출한다(바이트코드로 확인).
     * 이 클래스가 옮겨지거나 규칙이 바뀌면 이 테스트가 깨져서 알려준다.
     */
    private static int publisherPartition(String stockCode, int partitionCount) {
        byte[] keyBytes = stockCode.getBytes(StandardCharsets.UTF_8);
        return BuiltInPartitioner.partitionForKey(keyBytes, partitionCount);
    }

    @Mock @SuppressWarnings("rawtypes") KafkaTemplate kafkaTemplate;

    private static final List<String> STOCK_CODES =
        List.of("005930", "000660", "035420", "068270", "207940", "005380", "051910", "A900110");

    @Test
    @DisplayName("종목 코드가 가는 파티션 번호는 발행 쪽 계산과 같다")
    void 파티션_번호가_발행_쪽과_같다() {
        int partitionCount = 50;

        STOCK_CODES.forEach(stockCode -> {
            int expected = publisherPartition(stockCode, partitionCount);
            int actual = StockPartitionResolver.partitionFor(stockCode, partitionCount);
            assertThat(actual)
                .as("종목 %s", stockCode)
                .isEqualTo(expected);
        });
    }

    @Test
    @DisplayName("파티션 수가 달라져도 발행 쪽 계산과 같다")
    void 파티션_수가_달라도_발행_쪽과_같다() {
        List<Integer> partitionCounts = List.of(1, 3, 12, 50, 128);

        partitionCounts.forEach(partitionCount -> STOCK_CODES.forEach(stockCode -> {
            int expected = publisherPartition(stockCode, partitionCount);
            int actual = StockPartitionResolver.partitionFor(stockCode, partitionCount);
            assertThat(actual)
                .as("종목 %s, 파티션 수 %d", stockCode, partitionCount)
                .isEqualTo(expected);
        }));
    }

    @Test
    @DisplayName("파티션 번호는 0 이상 파티션 수 미만")
    void 파티션_번호_범위() {
        int partitionCount = 50;

        STOCK_CODES.forEach(stockCode -> {
            int partition = StockPartitionResolver.partitionFor(stockCode, partitionCount);
            assertThat(partition).isBetween(0, partitionCount - 1);
        });
    }

    @Test
    @DisplayName("주어진 파티션 번호에 속하는 종목만 골라낸다")
    void 파티션에_속하는_종목만_반환() {
        int partitionCount = 50;
        givenOrdersTopicPartitions(partitionCount);
        StockPartitionResolver resolver = new StockPartitionResolver(kafkaTemplate);

        int samsungPartition = publisherPartition("005930", partitionCount);
        int naverPartition = publisherPartition("035420", partitionCount);
        Set<Integer> assigned = Set.of(samsungPartition, naverPartition);

        List<String> filtered = resolver.filterByPartitions(STOCK_CODES, assigned);

        assertThat(filtered).contains("005930", "035420");
        assertThat(filtered).doesNotContain("000660", "068270", "207940");
    }

    @Test
    @DisplayName("파티션 수를 하드코딩하지 않고 orders 토픽 메타데이터에서 읽는다")
    void 파티션_수를_메타데이터에서_읽는다() {
        int partitionCount = 12;
        givenOrdersTopicPartitions(partitionCount);
        StockPartitionResolver resolver = new StockPartitionResolver(kafkaTemplate);

        int samsungPartition = publisherPartition("005930", partitionCount);
        List<String> filtered = resolver.filterByPartitions(STOCK_CODES, Set.of(samsungPartition));

        assertThat(filtered).contains("005930");
        // 파티션 수를 50 으로 착각하면 "005930" 은 6번으로 계산돼 이 조건을 통과하지 못한다.
        assertThat(samsungPartition).isNotEqualTo(publisherPartition("005930", 50));
    }

    @Test
    @DisplayName("반납된 파티션이 없으면 브로커 메타데이터를 조회하지 않는다")
    void 빈_파티션_집합이면_조회하지_않는다() {
        StockPartitionResolver resolver = new StockPartitionResolver(kafkaTemplate);

        List<String> filtered = resolver.filterByPartitions(STOCK_CODES, Set.of());

        assertThat(filtered).isEmpty();
    }

    @SuppressWarnings("unchecked")
    private void givenOrdersTopicPartitions(int partitionCount) {
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int partition = 0; partition < partitionCount; partition++) {
            partitions.add(new PartitionInfo("orders", partition, null, new org.apache.kafka.common.Node[0],
                new org.apache.kafka.common.Node[0]));
        }
        when(kafkaTemplate.partitionsFor("orders")).thenReturn(partitions);
    }
}
