package com.flab.stocktradingengine.settlement.engine.kafka.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.math.BigDecimal;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import com.flab.stocktradingengine.kafka.event.TradeFilledEvent;
import com.flab.stocktradingengine.settlement.service.OrderSettlementService;
import com.flab.stocktradingengine.time.EpochNanos;
import com.flab.stocktradingengine.time.StageLatencyRegistry;
import com.flab.stocktradingengine.time.StageSnapshot;

/**
 * 구간 분해 측정의 v1 settlement-engine 지점(V-P11~V-P12). 체결 한 건은 정산 반영이 끝난 뒤 표본 한 건으로
 * 기록되고(v2의 매수 체결 이벤트 기록과 같은 모집단), 반영이 실패하거나 방출 시각이 비면 기록하지 않는다.
 */
class SettlementSegmentStagesTest {

    private static final String[] FILL_STAGES = {"fill_transit", "E", "F"};

    private final OrderSettlementService service = mock(OrderSettlementService.class);
    private final Acknowledgment ack = mock(Acknowledgment.class);

    private SettlementConsumer consumer(StageLatencyRegistry registry) {
        SettlementConsumer consumer = new SettlementConsumer(service);
        consumer.setStageRegistry(registry);
        return consumer;
    }

    private static ConsumerRecord<String, Object> record(TradeFilledEvent event) {
        return new ConsumerRecord<>("fills", 0, 0L, "005930", event);
    }

    private static TradeFilledEvent fill(long emittedAtNanos, long aggressorOriginNanos) {
        return new TradeFilledEvent(9001L, "005930", 1L, 100L, 2L, 200L, 1, new BigDecimal("70000"),
            emittedAtNanos, aggressorOriginNanos);
    }

    private static long count(Map<String, StageSnapshot> snapshots, String stage) {
        StageSnapshot snapshot = snapshots.get(stage);
        return snapshot == null ? 0L : snapshot.count();
    }

    @Test
    void 정산_반영이_끝나면_세_단계가_한_건씩_기록되고_F는_방출까지_구간과_E의_합이다() {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        long origin = EpochNanos.now() - 1_000_000L;
        long emitted = EpochNanos.now() - 200_000L;

        consumer(registry).consume(record(fill(emitted, origin)), ack);

        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();
        for (String stage : FILL_STAGES) {
            assertThat(count(snapshots, stage)).as(stage).isEqualTo(1L);
        }
        double expectedFMicros = (emitted - origin) / 1000.0 + snapshots.get("E").meanMicros();
        double actualFMicros = snapshots.get("F").meanMicros();
        assertThat(Math.abs(actualFMicros - expectedFMicros) / actualFMicros).isLessThanOrEqualTo(0.01);
    }

    @Test
    void 방출_시각이_0인_옛_체결은_기록하지_않는다() {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);

        consumer(registry).consume(record(fill(0L, 0L)), ack);

        for (String stage : FILL_STAGES) {
            assertThat(count(registry.snapshotAndResetAll(), stage)).as(stage).isZero();
        }
    }

    @Test
    void 정산_반영이_폐기되면_기록하지_않는다() {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        doThrow(new IllegalArgumentException("체결 수량 초과")).when(service).fillTradePartially(anyLong(), anyLong(), anyLong(), anyInt(), any());

        consumer(registry).consume(record(fill(EpochNanos.now() - 1000L, EpochNanos.now() - 5000L)), ack);

        for (String stage : FILL_STAGES) {
            assertThat(count(registry.snapshotAndResetAll(), stage)).as(stage).isZero();
        }
    }

    @Test
    void 계측을_끄면_기록하지_않는다() {
        StageLatencyRegistry registry = new StageLatencyRegistry(false);

        consumer(registry).consume(record(fill(EpochNanos.now() - 1000L, EpochNanos.now() - 5000L)), ack);

        for (String stage : FILL_STAGES) {
            assertThat(count(registry.snapshotAndResetAll(), stage)).as(stage).isZero();
        }
    }
}
