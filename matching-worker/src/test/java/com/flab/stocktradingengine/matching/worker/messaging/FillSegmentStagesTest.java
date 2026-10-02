package com.flab.stocktradingengine.matching.worker.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.flab.stocktradingengine.aeron.ShardRoutingTable;
import com.flab.stocktradingengine.aeron.ShardRoutingTable.ShardRange;
import com.flab.stocktradingengine.aeron.StaticShardDestinationResolver;
import com.flab.stocktradingengine.codec.FillCodec;
import com.flab.stocktradingengine.codec.FilledTrade;
import com.flab.stocktradingengine.support.SnowflakeIdGenerator;
import com.flab.stocktradingengine.time.EpochNanos;
import com.flab.stocktradingengine.time.StageLatencyRegistry;
import com.flab.stocktradingengine.time.StageSnapshot;
import com.flab.stocktradingengine.trading.matching.FillResult;

import io.aeron.ExclusivePublication;

/**
 * 구간 분해 측정의 체결 발행 지점(W-P14 전달, W-P15~W-P16). 방출 시각과 aggressor 발신 시각이 와이어에
 * 실리는지, 발신 스레드가 꺼내 가기까지(fill_queue)와 offer에 걸린 시간(fill_offer)이 기록되는지 본다.
 */
class FillSegmentStagesTest {

    private static final String STOCK = "005930";
    private static final String ENDPOINT = "aeron:udp?endpoint=localhost:6001";
    private static final long VERIFY_TIMEOUT_MILLIS = 1000L;

    private AccountFillPublisher publisher;

    @AfterEach
    void tearDown() {
        if (publisher != null) {
            publisher.close();
        }
    }

    private AccountFillPublisher start(ExclusivePublication publication, StageLatencyRegistry registry) {
        ShardRoutingTable table = new ShardRoutingTable(1, List.of(new ShardRange(ENDPOINT, 0, 0)));
        SnowflakeIdGenerator ids = mock(SnowflakeIdGenerator.class);
        when(ids.nextId()).thenReturn(9001L);
        publisher = new AccountFillPublisher(new StaticShardDestinationResolver(table), Map.of(ENDPOINT, publication), ids, registry);
        publisher.start();
        return publisher;
    }

    @Test
    void 시각을_실은_체결은_와이어_디코딩에서_두_시각이_그대로_나온다() {
        ExclusivePublication publication = mock(ExclusivePublication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(100L);
        AccountFillPublisher started = start(publication, new StageLatencyRegistry(false));

        started.onFill(STOCK, new FillResult(1001L, 1L, 2001L, 2L, 4, new BigDecimal("10000")), 777L, 555L);

        ArgumentCaptor<DirectBuffer> buffer = ArgumentCaptor.forClass(DirectBuffer.class);
        verify(publication, timeout(VERIFY_TIMEOUT_MILLIS)).offer(buffer.capture(), eq(0), anyInt());
        FilledTrade decoded = new FillCodec().decode(buffer.getValue(), 0);
        assertThat(decoded.emittedAtNanos()).isEqualTo(777L);
        assertThat(decoded.aggressorOriginNanos()).isEqualTo(555L);
    }

    @Test
    void 켜진_상태에서_방출_시각이_있는_체결은_fill_queue와_fill_offer를_한_건씩_기록한다() {
        ExclusivePublication publication = mock(ExclusivePublication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(100L);
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        AccountFillPublisher started = start(publication, registry);

        started.onFill(STOCK, new FillResult(1001L, 1L, 2001L, 2L, 4, new BigDecimal("10000")),
            EpochNanos.now(), EpochNanos.now() - 10_000);
        verify(publication, timeout(VERIFY_TIMEOUT_MILLIS)).offer(any(DirectBuffer.class), eq(0), anyInt());

        awaitCount(registry, "fill_offer", 1);
        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();
        assertThat(snapshots.get("fill_queue").count()).isEqualTo(1L);
        assertThat(snapshots.get("fill_offer").count()).isEqualTo(1L);
    }

    @Test
    void 방출_시각이_0인_체결은_기록하지_않는다() {
        ExclusivePublication publication = mock(ExclusivePublication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(100L);
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        AccountFillPublisher started = start(publication, registry);

        started.onFill(STOCK, new FillResult(1001L, 1L, 2001L, 2L, 4, new BigDecimal("10000")), 0L, 0L);
        verify(publication, timeout(VERIFY_TIMEOUT_MILLIS)).offer(any(DirectBuffer.class), eq(0), anyInt());

        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();
        assertThat(snapshots.get("fill_queue").count()).isZero();
        assertThat(snapshots.get("fill_offer").count()).isZero();
    }

    @Test
    void 두_인자_호출은_두_시각을_0으로_보낸다() {
        ExclusivePublication publication = mock(ExclusivePublication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(100L);
        AccountFillPublisher started = start(publication, new StageLatencyRegistry(false));

        started.onFill(STOCK, new FillResult(1001L, 1L, 2001L, 2L, 4, new BigDecimal("10000")));

        ArgumentCaptor<DirectBuffer> buffer = ArgumentCaptor.forClass(DirectBuffer.class);
        verify(publication, timeout(VERIFY_TIMEOUT_MILLIS)).offer(buffer.capture(), eq(0), anyInt());
        assertThat(new FillCodec().decode(buffer.getValue(), 0).emittedAtNanos()).isZero();
    }

    private static void awaitCount(StageLatencyRegistry registry, String stage, long expected) {
        // 스냅샷은 비우므로 호출 전 다시 채우지 못한다 — 발신 스레드가 offer 뒤 기록하는 짧은 틈만 기다린다.
        try {
            Thread.sleep(150);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
