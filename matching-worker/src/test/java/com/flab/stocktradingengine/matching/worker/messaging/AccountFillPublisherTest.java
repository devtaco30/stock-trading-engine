package com.flab.stocktradingengine.matching.worker.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.flab.stocktradingengine.codec.FillCodec;
import com.flab.stocktradingengine.codec.FilledTrade;
import com.flab.stocktradingengine.support.SnowflakeIdGenerator;
import com.flab.stocktradingengine.trading.matching.FillResult;

import io.aeron.ExclusivePublication;

/**
 * 체결 하나가 tradeId 한 번만 발급돼 Aeron 스트림에 never-drop offer로 딱 한 번 발행되는지
 * 검증한다(ADR-032, U2 — Kafka fan-out 제거. IPC 단일 스트림엔 파티션이 없어 두 번 보낼 이유가
 * 없다. 계좌 수신기가 이 메시지 1건으로 매수·매도 양쪽을 반영한다).
 */
class AccountFillPublisherTest {

    private static final String STOCK = "005930";

    @Test
    void 체결하나를_never_drop_offer로_한번만_발행하고_fan_out하지_않는다() {
        ExclusivePublication publication = mock(ExclusivePublication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(100L);
        SnowflakeIdGenerator snowflakeIdGenerator = mock(SnowflakeIdGenerator.class);
        when(snowflakeIdGenerator.nextId()).thenReturn(9001L);

        AccountFillPublisher publisher = new AccountFillPublisher(publication, snowflakeIdGenerator);
        FillResult fill = new FillResult(1001L, 1L, 2001L, 2L, 4, new BigDecimal("10000"));

        publisher.onFill(STOCK, fill);

        verify(snowflakeIdGenerator, times(1)).nextId();
        ArgumentCaptor<DirectBuffer> bufferCaptor = ArgumentCaptor.forClass(DirectBuffer.class);
        ArgumentCaptor<Integer> lengthCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(publication, times(1)).offer(bufferCaptor.capture(), eq(0), lengthCaptor.capture());

        FillCodec codec = new FillCodec();
        FilledTrade decoded = codec.decode(bufferCaptor.getValue(), 0);
        FilledTrade expected = new FilledTrade(9001L, STOCK, 1001L, 1L, 2001L, 2L, 4, new BigDecimal("10000"));
        assertThat(decoded).isEqualTo(expected);
    }
}
