package com.flab.stocktradingengine.matching.kafka.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;

import com.flab.stocktradingengine.kafka.event.OrderPlacedEvent;
import com.flab.stocktradingengine.kafka.event.TradeFilledEvent;
import com.flab.stocktradingengine.matching.kafka.partition.StockPartitionResolver;
import com.flab.stocktradingengine.matching.redis.LtpRedisRepository;
import com.flab.stocktradingengine.matching.redis.OrderbookRedisRepository;
import com.flab.stocktradingengine.support.SnowflakeIdGenerator;
import com.flab.stocktradingengine.time.EpochNanos;
import com.flab.stocktradingengine.time.StageLatencyRegistry;
import com.flab.stocktradingengine.time.StageSnapshot;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.matching.OrderBookRegistry;
import com.flab.stocktradingengine.trading.service.OrderQueryService;

/**
 * 구간 분해 측정의 v1 matching-engine 지점(V-P9~V-P10). C는 접수 완료된 주문마다, D와 aggressor 기준
 * A·B·C는 체결을 일으킨 주문의 첫 체결에서만 기록되고, 체결 이벤트에 방출 시각과 aggressor 요청 시각이 실린다.
 */
class MatchingSegmentStagesTest {

    private static final String STOCK = "005930";

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
    private final SnowflakeIdGenerator ids = mock(SnowflakeIdGenerator.class);
    private final Acknowledgment ack = mock(Acknowledgment.class);

    private MatchingConsumer consumer(StageLatencyRegistry registry) {
        when(ids.nextId()).thenReturn(9001L);
        MatchingConsumer consumer = new MatchingConsumer(
            kafkaTemplate, new OrderBookRegistry(), mock(OrderQueryService.class),
            mock(LtpRedisRepository.class), mock(OrderbookRedisRepository.class), ids,
            new StockPartitionResolver(kafkaTemplate));
        consumer.setStageRegistry(registry);
        return consumer;
    }

    private static Instant microsAgo(long micros) {
        return Instant.now().minus(micros, ChronoUnit.MICROS).truncatedTo(ChronoUnit.MICROS);
    }

    private static ConsumerRecord<String, Object> record(OrderPlacedEvent event) {
        return new ConsumerRecord<>("orders", 0, 0L, STOCK, event);
    }

    /** 접수 완료까지 거친 주문 이벤트. 요청 시각 기준 +50µs에 수신, +150µs에 접수 완료로 만든다. */
    private static OrderPlacedEvent accepted(long orderId, long accountId, OrderSide side, long originMicrosAgo) {
        Instant requestedAt = microsAgo(originMicrosAgo);
        long origin = EpochNanos.of(requestedAt);
        return new OrderPlacedEvent(orderId, accountId, STOCK, side, new BigDecimal("70000"), 1, requestedAt,
            origin + 50_000L, origin + 150_000L);
    }

    private static long count(Map<String, StageSnapshot> snapshots, String stage) {
        StageSnapshot snapshot = snapshots.get(stage);
        return snapshot == null ? 0L : snapshot.count();
    }

    @Test
    void 체결이_나면_C는_주문마다_D와_aggressor_기준_세_단계는_한_건씩_기록된다() {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        MatchingConsumer consumer = consumer(registry);

        consumer.consume(record(accepted(1L, 100L, OrderSide.SELL, 600)), ack);
        consumer.consume(record(accepted(2L, 200L, OrderSide.BUY, 500)), ack);

        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();
        assertThat(count(snapshots, "C")).isEqualTo(2L);
        for (String stage : new String[] {"D", "A@aggr", "B@aggr", "C@aggr"}) {
            assertThat(count(snapshots, stage)).as(stage).isEqualTo(1L);
        }
    }

    @Test
    void 체결_이벤트에_방출_시각과_aggressor_요청_시각이_실리고_네_단계_합과_일치한다() {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        MatchingConsumer consumer = consumer(registry);
        OrderPlacedEvent aggressor = accepted(2L, 200L, OrderSide.BUY, 500);

        consumer.consume(record(accepted(1L, 100L, OrderSide.SELL, 600)), ack);
        consumer.consume(record(aggressor), ack);

        ArgumentCaptor<Object> sent = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate).send(eq("fills"), eq(STOCK), sent.capture());
        TradeFilledEvent fill = (TradeFilledEvent) sent.getValue();
        assertThat(fill.aggressorOriginNanos()).isEqualTo(EpochNanos.of(aggressor.orderAt()));
        assertThat(fill.emittedAtNanos()).isGreaterThan(fill.aggressorOriginNanos());

        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();
        double partsMicros = snapshots.get("A@aggr").meanMicros() + snapshots.get("B@aggr").meanMicros()
            + snapshots.get("C@aggr").meanMicros() + snapshots.get("D").meanMicros();
        double wholeMicros = (fill.emittedAtNanos() - fill.aggressorOriginNanos()) / 1000.0;
        assertThat(Math.abs(partsMicros - wholeMicros) / wholeMicros).as("네 단계 합 %s, 방출-요청 %s", partsMicros, wholeMicros)
            .isLessThanOrEqualTo(0.01);
    }

    @Test
    void 체결이_없는_주문은_C까지만_기록한다() {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        MatchingConsumer consumer = consumer(registry);

        consumer.consume(record(accepted(1L, 100L, OrderSide.SELL, 600)), ack);

        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();
        assertThat(count(snapshots, "C")).isEqualTo(1L);
        for (String stage : new String[] {"D", "A@aggr", "B@aggr", "C@aggr"}) {
            assertThat(count(snapshots, stage)).as(stage).isZero();
        }
    }

    @Test
    void 접수_시각이_비어_있는_옛_메시지는_기록하지_않지만_매칭은_그대로_한다() {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        MatchingConsumer consumer = consumer(registry);
        Instant requestedAt = microsAgo(500);
        OrderPlacedEvent oldSell = new OrderPlacedEvent(1L, 100L, STOCK, OrderSide.SELL, new BigDecimal("70000"), 1, requestedAt);
        OrderPlacedEvent oldBuy = new OrderPlacedEvent(2L, 200L, STOCK, OrderSide.BUY, new BigDecimal("70000"), 1, requestedAt);

        consumer.consume(record(oldSell), ack);
        consumer.consume(record(oldBuy), ack);

        verify(kafkaTemplate, times(1)).send(eq("fills"), eq(STOCK), any(TradeFilledEvent.class));
        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();
        for (String stage : new String[] {"C", "D", "A@aggr", "B@aggr", "C@aggr"}) {
            assertThat(count(snapshots, stage)).as(stage).isZero();
        }
    }

    @Test
    void 계측을_끄면_기록하지_않고_체결_이벤트의_시각은_0이다() {
        StageLatencyRegistry registry = new StageLatencyRegistry(false);
        MatchingConsumer consumer = consumer(registry);

        consumer.consume(record(accepted(1L, 100L, OrderSide.SELL, 600)), ack);
        consumer.consume(record(accepted(2L, 200L, OrderSide.BUY, 500)), ack);

        ArgumentCaptor<Object> sent = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate).send(eq("fills"), eq(STOCK), sent.capture());
        assertThat(((TradeFilledEvent) sent.getValue()).emittedAtNanos()).isZero();
        assertThat(count(registry.snapshotAndResetAll(), "C")).isZero();
        assertThat(List.of(ack)).isNotEmpty();
    }
}
