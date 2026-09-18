package com.flab.stocktradingengine.matching.kafka.partition;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.flab.stocktradingengine.kafka.KafkaTopics;

import lombok.RequiredArgsConstructor;

/**
 * 종목 코드가 orders 토픽의 몇 번 파티션으로 가는지 계산한다.
 *
 * <p>발행 쪽(order-engine {@code OrderRequestConsumer})은 {@code key=stockCode} 로 보내고
 * {@code partitioner.class} 를 지정하지 않는다. 그래서 Kafka 기본 계산이 적용된다 —
 * 키를 UTF-8 로 직렬화(StringSerializer)한 뒤 murmur2 해시를 파티션 수로 나눈 나머지다.
 * 컨슈머가 파티션만 알고 종목을 모르는 상황에서 복원 대상을 고르려면 같은 계산이 필요하다.</p>
 */
@Component
@RequiredArgsConstructor
public class StockPartitionResolver {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * 종목 코드가 가는 파티션 번호.
     *
     * @param stockCode      Kafka 파티션 키로 쓰이는 종목 코드
     * @param partitionCount orders 토픽의 파티션 수
     * @return 0 이상 {@code partitionCount} 미만의 파티션 번호
     */
    public static int partitionFor(String stockCode, int partitionCount) {
        byte[] keyBytes = stockCode.getBytes(StandardCharsets.UTF_8);
        int hash = Utils.murmur2(keyBytes);
        int positiveHash = Utils.toPositive(hash);
        return positiveHash % partitionCount;
    }

    /**
     * 주어진 종목 중 {@code partitionNumbers} 에 속하는 것만 골라낸다.
     * 파티션 수는 orders 토픽 메타데이터에서 읽는다(하드코딩하면 토픽 설정과 어긋난다).
     *
     * @param stockCodes       후보 종목 코드
     * @param partitionNumbers 할당받았거나 반납한 파티션 번호
     * @return 그 파티션들에 실리는 종목 코드
     */
    public List<String> filterByPartitions(Collection<String> stockCodes, Set<Integer> partitionNumbers) {
        if (stockCodes.isEmpty() || partitionNumbers.isEmpty()) {
            return List.of();
        }
        int partitionCount = ordersPartitionCount();
        return stockCodes.stream()
            .filter(stockCode -> {
                int partition = partitionFor(stockCode, partitionCount);
                return partitionNumbers.contains(partition);
            })
            .toList();
    }

    private int ordersPartitionCount() {
        List<PartitionInfo> partitions = kafkaTemplate.partitionsFor(KafkaTopics.orders());
        return partitions.size();
    }
}
