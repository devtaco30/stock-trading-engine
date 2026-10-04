package com.flab.stocktradingengine.matching.worker.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.ShardRoutingTable;
import com.flab.stocktradingengine.aeron.ShardRoutingTable.ShardRange;
import com.flab.stocktradingengine.aeron.StaticShardDestinationResolver;
import com.flab.stocktradingengine.codec.FillCodec;
import com.flab.stocktradingengine.matching.worker.id.MonotonicSnowflakeIdGenerator;
import com.flab.stocktradingengine.trading.matching.FillResult;

import io.aeron.ExclusivePublication;

/**
 * 시계가 뒤로 간 14:11:06 사건(0.14초 역행)에서 체결을 처리하는 스레드가 죽은 경로를,
 * 새 발급기를 꽂은 {@link AccountFillPublisher#onFill}로 다시 밟아 죽지 않는지 확인한다.
 */
class AccountFillPublisherClockRegressionTest {

    private static final String STOCK = "005930";
    private static final String ENDPOINT = "aeron:udp?endpoint=localhost:6001";

    @Test
    void 시계가_뒤로_가도_onFill은_예외_없이_번호가_늘어나는_체결을_내보낸다() {
        AtomicLong clock = new AtomicLong(1_791_090_666_529L);
        MonotonicSnowflakeIdGenerator generator = new MonotonicSnowflakeIdGenerator(2, clock::get);
        ExclusivePublication publication = mock(ExclusivePublication.class);
        FillCodec codec = new FillCodec();
        List<Long> tradeIds = new CopyOnWriteArrayList<>();
        // 발신 스레드가 버퍼를 재사용하므로 offer가 불리는 순간에 번호를 읽어 둔다.
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenAnswer(invocation -> {
            tradeIds.add(codec.decode(invocation.getArgument(0), 0).tradeId());
            return 100L;
        });
        ShardRoutingTable routingTable = new ShardRoutingTable(1, List.of(new ShardRange(ENDPOINT, 0, 0)));
        AccountFillPublisher publisher = new AccountFillPublisher(
            new StaticShardDestinationResolver(routingTable), Map.of(ENDPOINT, publication), generator);
        publisher.start();
        FillResult fill = new FillResult(1001L, 1L, 2001L, 2L, 4, new BigDecimal("10000"));
        try {
            assertThatCode(() -> {
                publisher.onFill(STOCK, fill);
                clock.set(1_791_090_666_393L); // 0.136초 뒤로
                publisher.onFill(STOCK, fill);
                clock.set(1_791_090_664_696L); // 1.833초 뒤로
                publisher.onFill(STOCK, fill);
            }).doesNotThrowAnyException();

            verify(publication, timeout(1000L).times(3)).offer(any(DirectBuffer.class), eq(0), anyInt());
            assertThat(tradeIds).hasSize(3).isSortedAccordingTo(Long::compare).doesNotHaveDuplicates();
        } finally {
            publisher.close();
        }
    }
}
