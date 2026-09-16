package com.flab.stocktradingengine.api.messaging;

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
import com.flab.stocktradingengine.api.exception.OrderPublishException;
import com.flab.stocktradingengine.codec.AccountOrderCodec;
import com.flab.stocktradingengine.codec.DecodedAccountOrder;
import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.Publication;

/**
 * 동기 발신(HTTP 요청 스레드에서 직접 encode+offer) — offer가 성공할 때까지 짧게 재시도하고,
 * 재시도가 다 떨어지면 주문을 조용히 버리지 않고 예외를 던진다(ADR-032, fork5 U1b). 목적지로의
 * 라우팅 자체는 {@link AeronAccountOrderSenderRoutingTest}가 보므로, 여기서는 슬롯 1개짜리
 * 라우팅 테이블로 목적지를 고정해두고 발신 재시도·예외 동작만 본다.
 */
class AeronAccountOrderSenderTest {

    private static final long ACCOUNT_ID = 1L;
    private static final String STOCK_CODE = "005930";
    private static final String ENDPOINT = "aeron:ipc";
    private static final ShardRoutingTable SINGLE_SHARD_ROUTING_TABLE =
        new ShardRoutingTable(1, List.of(new ShardRange(ENDPOINT, 0, 0)));

    @Test
    void offer가_바로_성공하면_한번만_발신한다() {
        Publication publication = mock(Publication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(100L);
        AeronAccountOrderSender sender = new AeronAccountOrderSender(SINGLE_SHARD_ROUTING_TABLE, Map.of(ENDPOINT, publication));

        sender.send(OrderSide.BUY, ACCOUNT_ID, STOCK_CODE, new BigDecimal("10000"), 10, "req-1");

        ArgumentCaptor<DirectBuffer> bufferCaptor = ArgumentCaptor.forClass(DirectBuffer.class);
        ArgumentCaptor<Integer> lengthCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(publication, times(1)).offer(bufferCaptor.capture(), eq(0), lengthCaptor.capture());

        AccountOrderCodec codec = new AccountOrderCodec();
        DecodedAccountOrder decoded = codec.decode(bufferCaptor.getValue(), 0);
        DecodedAccountOrder expected = new DecodedAccountOrder(
            OrderSide.BUY, ACCOUNT_ID, STOCK_CODE, new BigDecimal("10000"), 10, "req-1", 0L);
        org.assertj.core.api.Assertions.assertThat(decoded)
            .usingRecursiveComparison().ignoringFields("publishedAtEpochNanos")
            .isEqualTo(expected);
    }

    @Test
    void offer가_일시_실패하면_짧게_재시도해_성공시킨다() {
        Publication publication = mock(Publication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt()))
            .thenReturn(Publication.BACK_PRESSURED, Publication.BACK_PRESSURED, 200L);
        AeronAccountOrderSender sender = new AeronAccountOrderSender(SINGLE_SHARD_ROUTING_TABLE, Map.of(ENDPOINT, publication));

        sender.send(OrderSide.SELL, ACCOUNT_ID, STOCK_CODE, new BigDecimal("10000"), 5, "req-2");

        verify(publication, times(3)).offer(any(DirectBuffer.class), anyInt(), anyInt());
    }

    @Test
    void 재시도가_다_떨어지면_예외를_던진다_주문을_버리지_않는다() {
        Publication publication = mock(Publication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(Publication.BACK_PRESSURED);
        AeronAccountOrderSender sender = new AeronAccountOrderSender(SINGLE_SHARD_ROUTING_TABLE, Map.of(ENDPOINT, publication));

        assertThatThrownBy(() -> sender.send(OrderSide.BUY, ACCOUNT_ID, STOCK_CODE, new BigDecimal("10000"), 10, "req-3"))
            .isInstanceOf(OrderPublishException.class);
    }
}
