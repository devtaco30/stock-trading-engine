package com.flab.stocktradingengine.api.messaging;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.ShardRoutingTable;
import com.flab.stocktradingengine.aeron.ShardRoutingTable.ShardRange;
import com.flab.stocktradingengine.aeron.StaticShardDestinationResolver;
import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.Publication;

/**
 * I8 U1 — api가 계좌번호로 목적지를 골라 보내는지 검증한다. {@link ShardRoutingTable}(순수
 * 로직)에 endpoint별 mock {@link Publication} 맵을 구성해, 서로 다른 슬롯에 속하는 계좌 둘의
 * 주문이 각자 다른 목적지로 나가는지 본다.
 */
class AeronAccountOrderSenderRoutingTest {

    private static final String STOCK = "005930";
    private static final String ENDPOINT_A = "aeron:udp?endpoint=localhost:20040";
    private static final String ENDPOINT_B = "aeron:udp?endpoint=localhost:20041";

    @Test
    void 서로_다른_슬롯의_계좌_주문이_각자_다른_목적지로_나간다() {
        ShardRoutingTable routingTable = new ShardRoutingTable(2, List.of(
            new ShardRange(ENDPOINT_A, 0, 0), new ShardRange(ENDPOINT_B, 1, 1)));
        long accountInSlotA = firstAccountIdInSlot(routingTable, 0);
        long accountInSlotB = firstAccountIdInSlot(routingTable, 1);

        Publication publicationA = mock(Publication.class);
        when(publicationA.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(100L);
        Publication publicationB = mock(Publication.class);
        when(publicationB.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(200L);

        AeronAccountOrderSender sender = new AeronAccountOrderSender(
            new StaticShardDestinationResolver(routingTable), Map.of(ENDPOINT_A, publicationA, ENDPOINT_B, publicationB));

        sender.send(OrderSide.BUY, accountInSlotA, STOCK, new BigDecimal("10000"), 10, "req-a");
        sender.send(OrderSide.SELL, accountInSlotB, STOCK, new BigDecimal("10000"), 5, "req-b");

        verify(publicationA, times(1)).offer(any(DirectBuffer.class), anyInt(), anyInt());
        verify(publicationB, times(1)).offer(any(DirectBuffer.class), anyInt(), anyInt());
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
