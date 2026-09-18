package com.flab.stocktradingengine.matching.kafka.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import com.flab.stocktradingengine.matching.kafka.partition.StockPartitionResolver;
import com.flab.stocktradingengine.matching.redis.LtpRedisRepository;
import com.flab.stocktradingengine.matching.redis.OrderbookRedisRepository;
import com.flab.stocktradingengine.support.SnowflakeIdGenerator;
import com.flab.stocktradingengine.trading.entity.Order;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.matching.OrderBook;
import com.flab.stocktradingengine.trading.matching.OrderBookRegistry;
import com.flab.stocktradingengine.trading.service.OrderQueryService;

/**
 * 리밸런스(파티션 할당·반납) 시 호가창 복원·제거 범위 검증.
 *
 * <p>orders 토픽은 하나이고 파티션이 여러 개라, 파티션 하나에 종목이 여럿 실린다.
 * 할당받은 파티션에 속하는 종목만 복원하고, 반납한 파티션에 속하는 종목만 제거해야 한다.</p>
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("MatchingConsumer 리밸런스 복원 테스트")
class MatchingConsumerRebalanceTest {

    private static final int PARTITION_COUNT = 50;
    private static final String TOPIC = "orders";

    private static final String SAMSUNG = "005930";
    private static final String HYNIX = "000660";
    private static final String NAVER = "035420";

    @Mock @SuppressWarnings("rawtypes") KafkaTemplate kafkaTemplate;
    @Mock OrderQueryService orderQueryService;
    @Mock LtpRedisRepository ltpRedisRepository;
    @Mock OrderbookRedisRepository orderbookRedisRepository;
    @Mock SnowflakeIdGenerator snowflakeIdGenerator;

    private OrderBookRegistry orderBookRegistry;
    private MatchingConsumer consumer;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        orderBookRegistry = new OrderBookRegistry();
        StockPartitionResolver stockPartitionResolver = new StockPartitionResolver(kafkaTemplate);
        consumer = new MatchingConsumer(
            kafkaTemplate, orderBookRegistry, orderQueryService,
            ltpRedisRepository, orderbookRedisRepository, snowflakeIdGenerator,
            stockPartitionResolver);

        List<PartitionInfo> partitions = new ArrayList<>();
        for (int partition = 0; partition < PARTITION_COUNT; partition++) {
            partitions.add(new PartitionInfo(TOPIC, partition, null, new Node[0], new Node[0]));
        }
        lenient().when(kafkaTemplate.partitionsFor(TOPIC)).thenReturn(partitions);
    }

    /** 발행 쪽(order-engine)이 쓰는 Kafka 기본 계산. 기대값을 만드는 기준. */
    private static int publisherPartition(String stockCode) {
        byte[] keyBytes = stockCode.getBytes(StandardCharsets.UTF_8);
        return BuiltInPartitioner.partitionForKey(keyBytes, PARTITION_COUNT);
    }

    private static TopicPartition topicPartition(String stockCode) {
        int partition = publisherPartition(stockCode);
        return new TopicPartition(TOPIC, partition);
    }

    private static Order pendingBuyOrder(long orderId, String stockCode, String price) {
        return Order.builder()
            .orderId(orderId)
            .accountId(100L)
            .stockCode(stockCode)
            .side(OrderSide.BUY)
            .price(new BigDecimal(price))
            .quantity(10)
            .orderAt(Instant.parse("2026-01-02T00:00:00Z"))
            .build();
    }

    // ── 파티션 할당 ──────────────────────────────────────────────────────────

    @Test
    @DisplayName("할당받은 파티션에 속한 종목만 호가창을 복원한다")
    void 할당_파티션_종목만_복원() {
        when(orderQueryService.getPendingStockCodes()).thenReturn(List.of(SAMSUNG, HYNIX, NAVER));
        when(orderQueryService.getPendingByStockCodeSortedByTime(SAMSUNG))
            .thenReturn(List.of(pendingBuyOrder(1L, SAMSUNG, "70000")));
        when(orderQueryService.getPendingByStockCodeSortedByTime(NAVER))
            .thenReturn(List.of(pendingBuyOrder(2L, NAVER, "200000")));

        Map<TopicPartition, Long> assignments = Map.of(
            topicPartition(SAMSUNG), 0L,
            topicPartition(NAVER), 0L);

        consumer.onPartitionsAssigned(assignments, null);

        OrderBook samsungBook = orderBookRegistry.get(SAMSUNG);
        assertThat(samsungBook).isNotNull();
        assertThat(samsungBook.containsOrder(1L)).isTrue();

        OrderBook naverBook = orderBookRegistry.get(NAVER);
        assertThat(naverBook).isNotNull();
        assertThat(naverBook.containsOrder(2L)).isTrue();

        assertThat(orderBookRegistry.get(HYNIX)).isNull();
        verify(orderQueryService, never()).getPendingByStockCodeSortedByTime(HYNIX);
    }

    @Test
    @DisplayName("PENDING 주문이 없는 종목은 복원 대상에 없다")
    void PENDING_없는_종목은_복원_안됨() {
        when(orderQueryService.getPendingStockCodes()).thenReturn(List.of());

        consumer.onPartitionsAssigned(Map.of(topicPartition(SAMSUNG), 0L), null);

        assertThat(orderBookRegistry.get(SAMSUNG)).isNull();
        verify(orderQueryService, never()).getPendingByStockCodeSortedByTime(SAMSUNG);
    }

    // ── 파티션 반납 ──────────────────────────────────────────────────────────

    @Test
    @DisplayName("반납한 파티션에 속한 종목의 호가창만 제거한다")
    void 반납_파티션_종목만_제거() {
        orderBookRegistry.getOrCreate(SAMSUNG);
        orderBookRegistry.getOrCreate(HYNIX);
        orderBookRegistry.getOrCreate(NAVER);
        orderBookRegistry.updateLastTradedPrice(SAMSUNG, new BigDecimal("70000"));
        orderBookRegistry.updateLastTradedPrice(HYNIX, new BigDecimal("150000"));

        consumer.onPartitionsRevoked(List.of(topicPartition(SAMSUNG)));

        assertThat(orderBookRegistry.get(SAMSUNG)).isNull();
        assertThat(orderBookRegistry.getLastTradedPrice(SAMSUNG)).isEmpty();

        assertThat(orderBookRegistry.get(HYNIX)).isNotNull();
        assertThat(orderBookRegistry.getLastTradedPrice(HYNIX)).isPresent();
        assertThat(orderBookRegistry.get(NAVER)).isNotNull();
    }

    @Test
    @DisplayName("파티션 반납은 DB 를 조회하지 않는다")
    void 반납은_DB_조회_없음() {
        orderBookRegistry.getOrCreate(SAMSUNG);

        consumer.onPartitionsRevoked(List.of(topicPartition(SAMSUNG)));

        verify(orderQueryService, never()).getPendingStockCodes();
    }

    @Test
    @DisplayName("같은 파티션에 실린 종목은 함께 할당되고 함께 반납된다")
    void 같은_파티션_종목은_함께_처리() {
        Set<String> sameSlot = Set.of("051910", "A900110");
        assertThat(publisherPartition("051910")).isEqualTo(publisherPartition("A900110"));

        sameSlot.forEach(stockCode -> orderBookRegistry.getOrCreate(stockCode));

        consumer.onPartitionsRevoked(List.of(topicPartition("051910")));

        assertThat(orderBookRegistry.get("051910")).isNull();
        assertThat(orderBookRegistry.get("A900110")).isNull();
    }
}
