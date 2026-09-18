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

import com.flab.stocktradingengine.aeron.AccountDestinationResolver;
import com.flab.stocktradingengine.aeron.ShardRoutingTable;
import com.flab.stocktradingengine.aeron.ShardRoutingTable.ShardRange;
import com.flab.stocktradingengine.aeron.StaticShardDestinationResolver;
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
    private static final AccountDestinationResolver SINGLE_SHARD_RESOLVER =
        new StaticShardDestinationResolver(new ShardRoutingTable(1, List.of(new ShardRange(ENDPOINT, 0, 0))));

    @Test
    void offer가_바로_성공하면_한번만_발신한다() {
        Publication publication = mock(Publication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(100L);
        AeronAccountOrderSender sender = new AeronAccountOrderSender(SINGLE_SHARD_RESOLVER, Map.of(ENDPOINT, publication));

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
        AeronAccountOrderSender sender = new AeronAccountOrderSender(SINGLE_SHARD_RESOLVER, Map.of(ENDPOINT, publication));

        sender.send(OrderSide.SELL, ACCOUNT_ID, STOCK_CODE, new BigDecimal("10000"), 5, "req-2");

        verify(publication, times(3)).offer(any(DirectBuffer.class), anyInt(), anyInt());
    }

    @Test
    void 재시도가_다_떨어지면_예외를_던진다_주문을_버리지_않는다() {
        Publication publication = mock(Publication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(Publication.BACK_PRESSURED);
        AeronAccountOrderSender sender = new AeronAccountOrderSender(SINGLE_SHARD_RESOLVER, Map.of(ENDPOINT, publication));

        assertThatThrownBy(() -> sender.send(OrderSide.BUY, ACCOUNT_ID, STOCK_CODE, new BigDecimal("10000"), 10, "req-3"))
            .isInstanceOf(OrderPublishException.class);
    }

    @Test
    void 목적지를_아직_찾지_못하면_offer를_시도하지_않고_예외를_던진다() {
        AccountDestinationResolver noDestinationResolver = resolverReturning(java.util.Optional.empty());
        AeronAccountOrderSender sender = new AeronAccountOrderSender(noDestinationResolver, Map.of(ENDPOINT, mock(Publication.class)));

        assertThatThrownBy(() -> sender.send(OrderSide.BUY, ACCOUNT_ID, STOCK_CODE, new BigDecimal("10000"), 10, "req-4"))
            .isInstanceOf(OrderPublishException.class);
    }

    @Test
    void 목적지가_있어도_풀에_없는_endpoint면_503으로_처리한다() {
        // 계좌 샤딩 U5 — account-shard-map이 shard-routing.endpoints 풀에 없는 endpoint를 가리키는
        // 설정 어긋남 상황. IllegalStateException(설정 오류)이 아니라 조용히 옛 목적지로 안 보내고
        // OrderPublishException(503)으로 처리해야 클라이언트가 재시도할 수 있다.
        AccountDestinationResolver unknownPoolResolver = resolverReturning(java.util.Optional.of("aeron:udp?endpoint=localhost:9999"));
        AeronAccountOrderSender sender = new AeronAccountOrderSender(unknownPoolResolver, Map.of(ENDPOINT, mock(Publication.class)));

        assertThatThrownBy(() -> sender.send(OrderSide.BUY, ACCOUNT_ID, STOCK_CODE, new BigDecimal("10000"), 10, "req-5"))
            .isInstanceOf(OrderPublishException.class);
    }

    /**
     * 주문·체결 목적지로 같은 값을 돌려주는 조회 창구. {@link AccountDestinationResolver}가 메서드
     * 둘(주문용·체결용)을 가지게 되어 람다로 못 만든다.
     */
    private static AccountDestinationResolver resolverReturning(java.util.Optional<String> endpoint) {
        return new AccountDestinationResolver() {

            @Override
            public java.util.Optional<String> orderEndpointFor(long accountId) {
                return endpoint;
            }

            @Override
            public java.util.Optional<String> fillEndpointFor(long accountId) {
                return endpoint;
            }
        };
    }
}
