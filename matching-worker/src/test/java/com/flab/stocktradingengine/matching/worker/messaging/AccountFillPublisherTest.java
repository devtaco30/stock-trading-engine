package com.flab.stocktradingengine.matching.worker.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.flab.stocktradingengine.aeron.AccountDestinationResolver;
import com.flab.stocktradingengine.aeron.ShardRoutingTable;
import com.flab.stocktradingengine.aeron.ShardRoutingTable.ShardRange;
import com.flab.stocktradingengine.aeron.StaticShardDestinationResolver;
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
 *
 * <p>I4부터 {@link AccountFillPublisher#onFill}은 {@link FillOutbox}에 큐잉만 하고 바로 돌아오므로,
 * 실제 Aeron {@code offer}는 endpoint 전용 스레드에서 비동기로 일어난다. account-worker
 * {@code AeronMatchingOrderSenderTest}가 쓰는 것과 같은 방식으로 {@code Mockito.timeout()}으로
 * 그 완료를 기다린다 — publisher 내부 큐·스레드 상태를 직접 들여다보지 않는다.</p>
 */
class AccountFillPublisherTest {

    private static final String STOCK = "005930";
    private static final String ENDPOINT_A = "aeron:udp?endpoint=localhost:6001";
    private static final String ENDPOINT_B = "aeron:udp?endpoint=localhost:6002";
    private static final long VERIFY_TIMEOUT_MILLIS = 1000L;

    private final FillCodec codec = new FillCodec();
    private final List<AccountFillPublisher> publishers = new ArrayList<>();

    @AfterEach
    void tearDown() {
        publishers.forEach(AccountFillPublisher::close);
    }

    @Test
    void 매수_매도가_같은_샤드면_한_목적지로_한번만_발행한다() {
        ShardRoutingTable routingTable = new ShardRoutingTable(1, List.of(new ShardRange(ENDPOINT_A, 0, 0)));
        ExclusivePublication publicationA = mock(ExclusivePublication.class);
        when(publicationA.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(100L);
        SnowflakeIdGenerator snowflakeIdGenerator = mock(SnowflakeIdGenerator.class);
        when(snowflakeIdGenerator.nextId()).thenReturn(9001L);

        AccountFillPublisher publisher = createStartedPublisher(
            routingTable, Map.of(ENDPOINT_A, publicationA), snowflakeIdGenerator);
        FillResult fill = new FillResult(1001L, 1L, 2001L, 2L, 4, new BigDecimal("10000"));

        publisher.onFill(STOCK, fill);

        verify(snowflakeIdGenerator, times(1)).nextId();
        ArgumentCaptor<DirectBuffer> bufferCaptor = ArgumentCaptor.forClass(DirectBuffer.class);
        verify(publicationA, timeout(VERIFY_TIMEOUT_MILLIS).times(1)).offer(bufferCaptor.capture(), eq(0), anyInt());
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

        AccountFillPublisher publisher = createStartedPublisher(
            routingTable, Map.of(ENDPOINT_A, publicationA, ENDPOINT_B, publicationB), snowflakeIdGenerator);
        FillResult fill = new FillResult(1001L, buyAccountId, 2001L, sellAccountId, 4, new BigDecimal("10000"));

        publisher.onFill(STOCK, fill);

        verify(snowflakeIdGenerator, times(1)).nextId();
        ArgumentCaptor<DirectBuffer> bufferA = ArgumentCaptor.forClass(DirectBuffer.class);
        ArgumentCaptor<DirectBuffer> bufferB = ArgumentCaptor.forClass(DirectBuffer.class);
        verify(publicationA, timeout(VERIFY_TIMEOUT_MILLIS).times(1)).offer(bufferA.capture(), eq(0), anyInt());
        verify(publicationB, timeout(VERIFY_TIMEOUT_MILLIS).times(1)).offer(bufferB.capture(), eq(0), anyInt());

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

        AccountFillPublisher publisher = createStartedPublisher(
            routingTable, Map.of(ENDPOINT_A, publication), snowflakeIdGenerator);
        FillResult fill = new FillResult(1001L, 1L, 2001L, 2L, 4, new BigDecimal("10000"));

        publisher.onFill(STOCK, fill);

        verify(publication, timeout(VERIFY_TIMEOUT_MILLIS).times(3)).offer(any(DirectBuffer.class), eq(0), anyInt());
    }

    @Test
    void 발신이_막혀도_onFill은_정해진_시간_안에_돌아온다() throws InterruptedException {
        // 발신 스레드를 일부러 start()하지 않는다 — onFill이 정말 큐잉만으로 끝나는지 보려는
        // 것이라, 다운스트림이 영원히 막혀 있는 컨슈머 스레드까지 띄우면 이 테스트 자체가
        // 그 스레드에 발이 묶인다(teardown이 드레인 타임아웃만큼 걸림).
        ShardRoutingTable routingTable = new ShardRoutingTable(1, List.of(new ShardRange(ENDPOINT_A, 0, 0)));
        ExclusivePublication publication = mock(ExclusivePublication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(Publication.BACK_PRESSURED);
        SnowflakeIdGenerator snowflakeIdGenerator = mock(SnowflakeIdGenerator.class);
        when(snowflakeIdGenerator.nextId()).thenReturn(9001L);

        AccountFillPublisher publisher = new AccountFillPublisher(
            new StaticShardDestinationResolver(routingTable), Map.of(ENDPOINT_A, publication), snowflakeIdGenerator);
        FillResult fill = new FillResult(1001L, 1L, 2001L, 2L, 4, new BigDecimal("10000"));

        Thread onFillThread = new Thread(() -> publisher.onFill(STOCK, fill));
        onFillThread.start();
        onFillThread.join(500);

        assertThat(onFillThread.isAlive()).isFalse();
    }

    @Test
    void 스트림이_CLOSED면_재시도_없이_포기한다() {
        ShardRoutingTable routingTable = new ShardRoutingTable(1, List.of(new ShardRange(ENDPOINT_A, 0, 0)));
        ExclusivePublication publication = mock(ExclusivePublication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(Publication.CLOSED);
        SnowflakeIdGenerator snowflakeIdGenerator = mock(SnowflakeIdGenerator.class);
        when(snowflakeIdGenerator.nextId()).thenReturn(9001L);

        AccountFillPublisher publisher = createStartedPublisher(
            routingTable, Map.of(ENDPOINT_A, publication), snowflakeIdGenerator);
        FillResult fill = new FillResult(1001L, 1L, 2001L, 2L, 4, new BigDecimal("10000"));

        publisher.onFill(STOCK, fill);

        // CLOSED는 복구 불가라 재시도 없이 즉시 포기한다 — offer는 정확히 한 번만 불린다.
        verify(publication, timeout(VERIFY_TIMEOUT_MILLIS).times(1)).offer(any(DirectBuffer.class), eq(0), anyInt());
    }

    @Test
    void 스트림이_복구_불가_상태면_다음_enqueue에서_매칭_스레드로_예외가_전파된다() {
        // 첫 onFill은 endpoint 전용 스레드가 CLOSED를 아직 감지하기 전이라 큐잉만 하고 정상
        // 반환한다(비동기). 그 스레드가 CLOSED를 감지해 치명 상태를 기록한 뒤에는, 같은
        // endpoint로 가는 다음 onFill이 매칭 소비자 스레드 자신에서 바로 예외를 던져야
        // MatchingExceptionHandler가 예전처럼 매칭 전체를 fail-fast로 멈출 수 있다.
        ShardRoutingTable routingTable = new ShardRoutingTable(1, List.of(new ShardRange(ENDPOINT_A, 0, 0)));
        ExclusivePublication publication = mock(ExclusivePublication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(Publication.CLOSED);
        SnowflakeIdGenerator snowflakeIdGenerator = mock(SnowflakeIdGenerator.class);
        when(snowflakeIdGenerator.nextId()).thenReturn(9001L);

        AccountFillPublisher publisher = createStartedPublisher(
            routingTable, Map.of(ENDPOINT_A, publication), snowflakeIdGenerator);
        FillResult fill = new FillResult(1001L, 1L, 2001L, 2L, 4, new BigDecimal("10000"));

        publisher.onFill(STOCK, fill);
        verify(publication, timeout(VERIFY_TIMEOUT_MILLIS).times(1)).offer(any(DirectBuffer.class), eq(0), anyInt());

        assertThatThrownBy(() -> publisher.onFill(STOCK, fill)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void 목적지가_있어도_풀에_없는_endpoint면_예외를_던진다() {
        // 계좌 샤딩 U5·U6 — account-shard-map이 shard-routing.endpoints 풀에 없는 endpoint를
        // 가리키는 설정 어긋남 상황. AccountDestinationMisconfiguredException은 IllegalStateException이
        // 아니라 MatchingEventHandler의 "이벤트만 폐기" catch를 피해 진짜 fail-fast로 간다.
        SnowflakeIdGenerator snowflakeIdGenerator = mock(SnowflakeIdGenerator.class);
        when(snowflakeIdGenerator.nextId()).thenReturn(9001L);
        AccountDestinationResolver unknownPoolResolver = accountId -> java.util.Optional.of("aeron:udp?endpoint=localhost:9999");
        AccountFillPublisher publisher = new AccountFillPublisher(
            unknownPoolResolver, Map.of(ENDPOINT_A, mock(ExclusivePublication.class)), snowflakeIdGenerator);
        publisher.start();
        publishers.add(publisher);
        FillResult fill = new FillResult(1001L, 1L, 2001L, 2L, 4, new BigDecimal("10000"));

        assertThatThrownBy(() -> publisher.onFill(STOCK, fill)).isInstanceOf(AccountDestinationMisconfiguredException.class);
    }

    private AccountFillPublisher createStartedPublisher(
            ShardRoutingTable routingTable,
            Map<String, ExclusivePublication> publicationsByEndpoint,
            SnowflakeIdGenerator snowflakeIdGenerator) {
        AccountFillPublisher publisher = new AccountFillPublisher(
            new StaticShardDestinationResolver(routingTable), publicationsByEndpoint, snowflakeIdGenerator);
        publisher.start();
        publishers.add(publisher);
        return publisher;
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
