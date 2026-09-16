package com.flab.stocktradingengine.matching.worker.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
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

import com.flab.stocktradingengine.aeron.ShardRoutingTable;
import com.flab.stocktradingengine.aeron.ShardRoutingTable.ShardRange;
import com.flab.stocktradingengine.support.SnowflakeIdGenerator;
import com.flab.stocktradingengine.trading.matching.FillResult;

import io.aeron.ExclusivePublication;
import io.aeron.Publication;

/**
 * fork3, I4 U2 — endpoint 하나가 영원히 못 받는 상태(dead)여도 다른 endpoint로 가는 발신은
 * 막히지 않는지 검증한다(D1, head-of-line blocking 방지). 매칭 단일 소비자 스레드가 이벤트를
 * 순서대로 처리하는 것을 그대로 흉내 내려고, 한 스레드 안에서 endpoint A행 체결 다음에 endpoint
 * B행 체결을 이어서 {@link AccountFillPublisher#onFill}로 넘긴다. U1 이전(공유 큐 없이 매칭
 * 소비자 스레드가 직접 offer를 재시도하던) 코드로 되돌려 이 테스트를 돌리면 A행 체결의 재시도
 * 루프에 그 스레드가 갇혀 B행 체결은 시도조차 되지 않는다 — 직접 확인했다(보고 참고).
 */
class AccountFillPublisherShardIsolationTest {

    private static final String STOCK = "005930";
    private static final String ENDPOINT_A = "aeron:udp?endpoint=localhost:6001";
    private static final String ENDPOINT_B = "aeron:udp?endpoint=localhost:6002";

    private AccountFillPublisher publisher;

    @AfterEach
    void tearDown() {
        if (publisher != null) {
            publisher.close();
        }
    }

    @Test
    void 한_샤드가_영원히_못_받아도_다른_샤드로는_계속_나간다() throws InterruptedException {
        ShardRoutingTable routingTable = new ShardRoutingTable(2, List.of(
            new ShardRange(ENDPOINT_A, 0, 0), new ShardRange(ENDPOINT_B, 1, 1)));
        long stuckAccountId = firstAccountIdInSlot(routingTable, 0);
        long healthyAccountId = firstAccountIdInSlot(routingTable, 1);

        ExclusivePublication publicationA = mock(ExclusivePublication.class);
        when(publicationA.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(Publication.BACK_PRESSURED);
        ExclusivePublication publicationB = mock(ExclusivePublication.class);
        when(publicationB.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(100L);
        SnowflakeIdGenerator snowflakeIdGenerator = mock(SnowflakeIdGenerator.class);
        when(snowflakeIdGenerator.nextId()).thenReturn(9001L, 9002L);

        publisher = new AccountFillPublisher(
            routingTable, Map.of(ENDPOINT_A, publicationA, ENDPOINT_B, publicationB), snowflakeIdGenerator);
        publisher.start();

        FillResult stuckFill = new FillResult(1001L, stuckAccountId, 2001L, stuckAccountId, 4, new BigDecimal("10000"));
        FillResult healthyFill = new FillResult(1002L, healthyAccountId, 2002L, healthyAccountId, 3, new BigDecimal("20000"));

        // 매칭 단일 소비자 스레드가 이벤트를 순서대로 넘기는 것을 흉내 낸다.
        Thread matchingConsumerThread = new Thread(() -> {
            publisher.onFill(STOCK, stuckFill);
            publisher.onFill(STOCK, healthyFill);
        });
        matchingConsumerThread.setDaemon(true);
        matchingConsumerThread.start();
        matchingConsumerThread.join(1000);

        assertThat(matchingConsumerThread.isAlive()).isFalse();
        verify(publicationB, timeout(1000).times(1)).offer(any(DirectBuffer.class), anyInt(), anyInt());
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
