package com.flab.stocktradingengine.matching.worker.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.flab.stocktradingengine.aeron.ShardRoutingTable;
import com.flab.stocktradingengine.aeron.ShardRoutingTable.ShardRange;
import com.flab.stocktradingengine.codec.FillCodec;
import com.flab.stocktradingengine.codec.FilledTrade;
import com.flab.stocktradingengine.support.SnowflakeIdGenerator;
import com.flab.stocktradingengine.trading.matching.FillResult;

import io.aeron.ExclusivePublication;
import io.aeron.Publication;

/**
 * fork3, Unit 2 — 체결이 매수·매도 계좌의 샤드 endpoint로 fan-out 발행되는지 검증한다. 실제
 * {@link ShardRoutingTable}(순수 로직)에 endpoint별 mock {@link ExclusivePublication} 맵을
 * 구성해 라우팅 계산과 발행 호출을 함께 검증한다(수신 모델 A — 소유 판단은 계좌측 몫이라
 * 여기서는 다루지 않는다).
 */
class AccountFillPublisherTest {

    private static final String STOCK = "005930";
    private static final String ENDPOINT_A = "aeron:udp?endpoint=localhost:6001";
    private static final String ENDPOINT_B = "aeron:udp?endpoint=localhost:6002";

    private final FillCodec codec = new FillCodec();

    @Test
    void 매수_매도가_같은_샤드면_한_목적지로_한번만_발행한다() {
        ShardRoutingTable routingTable = new ShardRoutingTable(1, List.of(new ShardRange(ENDPOINT_A, 0, 0)));
        ExclusivePublication publicationA = mock(ExclusivePublication.class);
        when(publicationA.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(100L);
        SnowflakeIdGenerator snowflakeIdGenerator = mock(SnowflakeIdGenerator.class);
        when(snowflakeIdGenerator.nextId()).thenReturn(9001L);

        AccountFillPublisher publisher = new AccountFillPublisher(
            routingTable, Map.of(ENDPOINT_A, publicationA), snowflakeIdGenerator);
        FillResult fill = new FillResult(1001L, 1L, 2001L, 2L, 4, new BigDecimal("10000"));

        publisher.onFill(STOCK, fill);

        verify(snowflakeIdGenerator, times(1)).nextId();
        ArgumentCaptor<DirectBuffer> bufferCaptor = ArgumentCaptor.forClass(DirectBuffer.class);
        verify(publicationA, times(1)).offer(bufferCaptor.capture(), eq(0), anyInt());
        FilledTrade decoded = codec.decode(bufferCaptor.getValue(), 0);
        assertThat(decoded).isEqualTo(new FilledTrade(9001L, STOCK, 1001L, 1L, 2001L, 2L, 4, new BigDecimal("10000")));
    }

    @Test
    void 매수_매도가_다른_샤드면_두_목적지에_각각_한번씩_발행한다() {
        ShardRoutingTable routingTable = new ShardRoutingTable(2, List.of(
            new ShardRange(ENDPOINT_A, 0, 0), new ShardRange(ENDPOINT_B, 1, 1)));
        long buyAccountId = firstAccountIdInSlot(routingTable, 0);
        long sellAccountId = firstAccountIdInSlot(routingTable, 1);
        ExclusivePublication publicationA = mock(ExclusivePublication.class);
        ExclusivePublication publicationB = mock(ExclusivePublication.class);
        when(publicationA.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(100L);
        when(publicationB.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(200L);
        SnowflakeIdGenerator snowflakeIdGenerator = mock(SnowflakeIdGenerator.class);
        when(snowflakeIdGenerator.nextId()).thenReturn(9001L);

        AccountFillPublisher publisher = new AccountFillPublisher(
            routingTable, Map.of(ENDPOINT_A, publicationA, ENDPOINT_B, publicationB), snowflakeIdGenerator);
        FillResult fill = new FillResult(1001L, buyAccountId, 2001L, sellAccountId, 4, new BigDecimal("10000"));

        publisher.onFill(STOCK, fill);

        verify(snowflakeIdGenerator, times(1)).nextId();
        ArgumentCaptor<DirectBuffer> bufferA = ArgumentCaptor.forClass(DirectBuffer.class);
        ArgumentCaptor<DirectBuffer> bufferB = ArgumentCaptor.forClass(DirectBuffer.class);
        verify(publicationA, times(1)).offer(bufferA.capture(), eq(0), anyInt());
        verify(publicationB, times(1)).offer(bufferB.capture(), eq(0), anyInt());

        FilledTrade expected = new FilledTrade(9001L, STOCK, 1001L, buyAccountId, 2001L, sellAccountId, 4, new BigDecimal("10000"));
        assertThat(codec.decode(bufferA.getValue(), 0)).isEqualTo(expected);
        assertThat(codec.decode(bufferB.getValue(), 0)).isEqualTo(expected);
    }

    @Test
    void 백프레셔면_재시도하다가_성공하면_끝난다() {
        ShardRoutingTable routingTable = new ShardRoutingTable(1, List.of(new ShardRange(ENDPOINT_A, 0, 0)));
        ExclusivePublication publication = mock(ExclusivePublication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt()))
            .thenReturn(Publication.BACK_PRESSURED, Publication.BACK_PRESSURED, 100L);
        SnowflakeIdGenerator snowflakeIdGenerator = mock(SnowflakeIdGenerator.class);
        when(snowflakeIdGenerator.nextId()).thenReturn(9001L);

        AccountFillPublisher publisher = new AccountFillPublisher(
            routingTable, Map.of(ENDPOINT_A, publication), snowflakeIdGenerator);
        FillResult fill = new FillResult(1001L, 1L, 2001L, 2L, 4, new BigDecimal("10000"));

        publisher.onFill(STOCK, fill);

        verify(publication, times(3)).offer(any(DirectBuffer.class), eq(0), anyInt());
    }

    @Test
    void 스트림이_CLOSED면_예외를_던진다() {
        ShardRoutingTable routingTable = new ShardRoutingTable(1, List.of(new ShardRange(ENDPOINT_A, 0, 0)));
        ExclusivePublication publication = mock(ExclusivePublication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(Publication.CLOSED);
        SnowflakeIdGenerator snowflakeIdGenerator = mock(SnowflakeIdGenerator.class);
        when(snowflakeIdGenerator.nextId()).thenReturn(9001L);

        AccountFillPublisher publisher = new AccountFillPublisher(
            routingTable, Map.of(ENDPOINT_A, publication), snowflakeIdGenerator);
        FillResult fill = new FillResult(1001L, 1L, 2001L, 2L, 4, new BigDecimal("10000"));

        assertThatThrownBy(() -> publisher.onFill(STOCK, fill)).isInstanceOf(IllegalStateException.class);
    }

    private long firstAccountIdInSlot(ShardRoutingTable table, int targetSlot) {
        for (long accountId = 0; accountId < 10_000; accountId++) {
            if (table.slotFor(accountId) == targetSlot) {
                return accountId;
            }
        }
        throw new IllegalStateException("탐색 범위 내에 슬롯 " + targetSlot + "에 해당하는 accountId가 없다");
    }
}
