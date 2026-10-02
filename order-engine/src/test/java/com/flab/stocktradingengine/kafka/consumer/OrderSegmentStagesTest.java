package com.flab.stocktradingengine.kafka.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;

import com.flab.stocktradingengine.kafka.event.OrderPlacedEvent;
import com.flab.stocktradingengine.kafka.event.OrderRequestEvent;
import com.flab.stocktradingengine.time.EpochNanos;
import com.flab.stocktradingengine.time.LatencyHistogram;
import com.flab.stocktradingengine.time.StageLatencyRegistry;
import com.flab.stocktradingengine.time.StageSnapshot;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.service.OrderCommandService;
import com.flab.stocktradingengine.trading.service.OrderStageTimes;
import com.flab.stocktradingengine.trading.view.PlaceOrderResultView;

/**
 * 구간 분해 측정의 order-engine 지점(V-P1, V-P3~V-P8). 접수 완료(커밋 포함)까지 간 주문만 기록하고,
 * 이미 접수된 요청이나 거부는 기록하지 않으며, 끄면 기존 호출 경로를 그대로 쓴다.
 */
class OrderSegmentStagesTest {

    private static final String STOCK = "005930";
    private static final String[] ACCEPT_STAGES = {
        "A", "b_idem", "b_lock", "b_sum", "b_decide", "b_insert", "B", "b_commit", "b_send_call"};

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
    private final OrderCommandService service = mock(OrderCommandService.class);
    private final Acknowledgment ack = mock(Acknowledgment.class);

    private OrderRequestConsumer consumer(StageLatencyRegistry registry) {
        OrderRequestConsumer consumer = new OrderRequestConsumer(service, kafkaTemplate, new LatencyHistogram(false));
        consumer.setStageRegistry(registry);
        return consumer;
    }

    private static OrderRequestEvent buyEvent(Instant requestedAt) {
        return new OrderRequestEvent(1001L, STOCK, OrderSide.BUY, "LIMIT", new BigDecimal("70000"), 1, requestedAt, "req-1");
    }

    private static ConsumerRecord<String, Object> record(Object value) {
        return new ConsumerRecord<>("order-requests", 0, 0L, "1001", value);
    }

    /** 서비스가 접수 경로를 끝까지 지난 것처럼 지점 시각을 채운다. */
    private void answerAsFullyAccepted() {
        PlaceOrderResultView view = new PlaceOrderResultView(8801L, "PENDING", 0L, BigDecimal.ONE);
        when(service.placeBuyOrder(any(), any(), any(OrderStageTimes.class))).thenAnswer(invocation -> {
            OrderStageTimes times = invocation.getArgument(2);
            times.markIdemChecked(EpochNanos.now());
            times.markLocked(EpochNanos.now());
            times.markSummed(EpochNanos.now());
            times.markDecided(EpochNanos.now());
            times.markInserted(EpochNanos.now());
            return view;
        });
    }

    private static long count(Map<String, StageSnapshot> snapshots, String stage) {
        StageSnapshot snapshot = snapshots.get(stage);
        return snapshot == null ? 0L : snapshot.count();
    }

    @Test
    void 접수_완료된_주문은_아홉_단계에_한_건씩_기록되고_이벤트에_시각이_실린다() {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        answerAsFullyAccepted();
        Instant requestedAt = Instant.now().minus(300, ChronoUnit.MICROS);

        consumer(registry).consume(record(buyEvent(requestedAt)), ack);

        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();
        for (String stage : ACCEPT_STAGES) {
            assertThat(count(snapshots, stage)).as(stage).isEqualTo(1L);
        }
        ArgumentCaptor<Object> sent = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate).send(eq("orders"), eq(STOCK), sent.capture());
        OrderPlacedEvent placed = (OrderPlacedEvent) sent.getValue();
        assertThat(placed.receivedAtNanos()).isGreaterThan(EpochNanos.of(requestedAt));
        assertThat(placed.acceptedAtNanos()).isGreaterThanOrEqualTo(placed.receivedAtNanos());
        verify(ack).acknowledge();
    }

    @Test
    void 이미_접수된_요청은_저장_지점이_비어_있어_기록하지_않는다() {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        PlaceOrderResultView view = new PlaceOrderResultView(8801L, "PENDING", 0L, BigDecimal.ONE);
        when(service.placeBuyOrder(any(), any(), any(OrderStageTimes.class))).thenAnswer(invocation -> {
            OrderStageTimes times = invocation.getArgument(2);
            times.markIdemChecked(EpochNanos.now());
            return view;
        });

        consumer(registry).consume(record(buyEvent(Instant.now())), ack);

        for (String stage : ACCEPT_STAGES) {
            assertThat(count(registry.snapshotAndResetAll(), stage)).as(stage).isZero();
        }
    }

    @Test
    void 계측을_끄면_기록하지_않고_시각_없는_서비스_호출을_쓴다() {
        StageLatencyRegistry registry = new StageLatencyRegistry(false);
        when(service.placeBuyOrder(any(), any()))
            .thenReturn(new PlaceOrderResultView(8801L, "PENDING", 0L, BigDecimal.ONE));

        consumer(registry).consume(record(buyEvent(Instant.now())), ack);

        verify(service).placeBuyOrder(any(), any());
        verify(service, never()).placeBuyOrder(any(), any(), any(OrderStageTimes.class));
        ArgumentCaptor<Object> sent = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate).send(eq("orders"), eq(STOCK), sent.capture());
        assertThat(((OrderPlacedEvent) sent.getValue()).receivedAtNanos()).isZero();
        for (String stage : ACCEPT_STAGES) {
            assertThat(count(registry.snapshotAndResetAll(), stage)).as(stage).isZero();
        }
    }
}
