package com.flab.stocktradingengine.api.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.AccountDestinationResolver;
import com.flab.stocktradingengine.aeron.ShardRoutingTable;
import com.flab.stocktradingengine.aeron.ShardRoutingTable.ShardRange;
import com.flab.stocktradingengine.aeron.StaticShardDestinationResolver;
import com.flab.stocktradingengine.api.exception.OrderPublishException;
import com.flab.stocktradingengine.time.StageLatencyRegistry;
import com.flab.stocktradingengine.time.StageSnapshot;
import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.Publication;

/**
 * 구간 분해 측정의 api 보조 지점(W-P1) — offer가 성공했을 때만 {@code api_send_cost}
 * (인코딩 + offer에 걸린 시간)를 한 건 기록하고, 계측을 끄면 아무것도 기록하지 않는다.
 */
class AeronAccountOrderSenderStageTest {

    private static final String ENDPOINT = "aeron:ipc";
    private static final AccountDestinationResolver RESOLVER =
        new StaticShardDestinationResolver(new ShardRoutingTable(1, List.of(new ShardRange(ENDPOINT, 0, 0))));

    private static Publication publicationReturning(long result) {
        Publication publication = mock(Publication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(result);
        return publication;
    }

    @Test
    void offer가_성공하면_api_send_cost에_한_건_기록한다() {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        AeronAccountOrderSender sender =
            new AeronAccountOrderSender(RESOLVER, Map.of(ENDPOINT, publicationReturning(100L)), registry);

        sender.send(OrderSide.BUY, 1L, "005930", new BigDecimal("10000"), 1, "req-1");

        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();
        assertThat(snapshots.get("api_send_cost").count()).isEqualTo(1L);
    }

    @Test
    void 재시도_뒤_성공해도_한_건만_기록한다() {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        Publication publication = mock(Publication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt()))
            .thenReturn(Publication.BACK_PRESSURED, 200L);
        AeronAccountOrderSender sender = new AeronAccountOrderSender(RESOLVER, Map.of(ENDPOINT, publication), registry);

        sender.send(OrderSide.BUY, 1L, "005930", new BigDecimal("10000"), 1, "req-2");

        assertThat(registry.snapshotAndResetAll().get("api_send_cost").count()).isEqualTo(1L);
    }

    @Test
    void 발신이_실패하면_기록하지_않는다() {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        AeronAccountOrderSender sender = new AeronAccountOrderSender(
            RESOLVER, Map.of(ENDPOINT, publicationReturning(Publication.BACK_PRESSURED)), registry);

        assertThatThrownBy(() -> sender.send(OrderSide.BUY, 1L, "005930", new BigDecimal("10000"), 1, "req-3"))
            .isInstanceOf(OrderPublishException.class);

        StageSnapshot snapshot = registry.snapshotAndResetAll().get("api_send_cost");
        assertThat(snapshot == null || snapshot.count() == 0L).isTrue();
    }

    @Test
    void 계측을_끄면_아무것도_기록하지_않는다() {
        StageLatencyRegistry registry = new StageLatencyRegistry(false);
        AeronAccountOrderSender sender =
            new AeronAccountOrderSender(RESOLVER, Map.of(ENDPOINT, publicationReturning(100L)), registry);

        sender.send(OrderSide.BUY, 1L, "005930", new BigDecimal("10000"), 1, "req-4");

        assertThat(registry.isEnabled()).isFalse();
        assertThat(registry.snapshotAndResetAll().values()).allMatch(s -> s.count() == 0L);
    }

    @Test
    void 레지스트리를_받지_않는_기존_생성자도_발신한다() {
        AeronAccountOrderSender sender =
            new AeronAccountOrderSender(RESOLVER, Map.of(ENDPOINT, publicationReturning(100L)));

        sender.send(OrderSide.BUY, 1L, "005930", new BigDecimal("10000"), 1, "req-5");
    }
}
