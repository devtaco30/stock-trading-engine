package com.flab.stocktradingengine.account.worker.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.List;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.Publication;

/**
 * publisherThread가 offer에 쓰는 인코딩 버퍼가 발행마다 새로 할당되지 않고 재사용되는지 검증한다
 * — off-heap(자바 힙 밖에 잡는 네이티브 메모리) 누적 방지(Cursor 1차 리뷰 ①).
 */
class AeronMatchingOrderSenderTest {

    private static final long ACCOUNT_ID = 1L;
    private static final String STOCK_CODE = "005930";

    private AeronMatchingOrderSender sender;

    @AfterEach
    void tearDown() {
        if (sender != null) {
            sender.close();
        }
    }

    @Test
    void 연속_발행_두_건이_같은_인코딩_버퍼_인스턴스를_재사용한다() {
        Publication publication = mock(Publication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(100L);
        sender = new AeronMatchingOrderSender(publication);
        sender.start();

        sender.forwardPlace(1L, ACCOUNT_ID, STOCK_CODE, OrderSide.BUY, new BigDecimal("10000"), 10);
        sender.forwardPlace(2L, ACCOUNT_ID, STOCK_CODE, OrderSide.BUY, new BigDecimal("10000"), 5);

        ArgumentCaptor<DirectBuffer> bufferCaptor = ArgumentCaptor.forClass(DirectBuffer.class);
        verify(publication, timeout(1000).times(2)).offer(bufferCaptor.capture(), anyInt(), anyInt());

        List<DirectBuffer> buffers = bufferCaptor.getAllValues();
        assertThat(buffers.get(0)).isSameAs(buffers.get(1));
    }
}
