package com.flab.stocktradingengine.account.worker.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.Publication;

/**
 * 매칭 샤딩 U9b — 매칭행 발신에서 세 값을 센다: queue에 쌓인 주문 수, 계좌 엔진 스레드가 queue에
 * 넣으려고 기다린 시간과 횟수, sender thread의 Aeron offer 재시도 횟수.
 */
class AeronMatchingOrderSenderMetricsTest {

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
    void sender_thread를_시작하지_않고_주문_세_건을_넣으면_쌓인_개수가_3이다() {
        Publication publication = mock(Publication.class);
        sender = new AeronMatchingOrderSender(publication);

        forward(1L);
        forward(2L);
        forward(3L);

        assertThat(sender.metrics().outboxSize()).isEqualTo(3);
        assertThat(sender.metrics().enqueueWaitCount()).isZero();
    }

    @Test
    void queue를_가득_채운_뒤_다른_스레드의_forwardPlace는_기다린_시간이_0보다_크고_sender를_시작하면_끝난다() throws InterruptedException {
        Publication publication = mock(Publication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(100L);
        sender = new AeronMatchingOrderSender(publication);
        for (int i = 0; i < AeronMatchingOrderSender.OUTBOX_CAPACITY; i++) {
            forward(i);
        }
        assertThat(sender.metrics().outboxSize()).isEqualTo(AeronMatchingOrderSender.OUTBOX_CAPACITY);

        Thread blockedProducer = new Thread(() -> forward(999_999L));
        blockedProducer.setDaemon(true);
        blockedProducer.start();
        blockedProducer.join(300);
        assertThat(blockedProducer.isAlive()).as("queue가 가득 차 기다리는 중").isTrue();

        sender.start();
        blockedProducer.join(5000);

        assertThat(blockedProducer.isAlive()).isFalse();
        assertThat(sender.metrics().enqueueWaitCount()).isEqualTo(1);
        assertThat(sender.metrics().enqueueWaitNanos()).isGreaterThan(0L);
    }

    @Test
    void offer가_일시적_실패를_두_번_돌려준_뒤_성공하면_재시도_횟수가_2다() {
        Publication publication = mock(Publication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt()))
            .thenReturn(Publication.BACK_PRESSURED, Publication.BACK_PRESSURED, 100L);
        sender = new AeronMatchingOrderSender(publication);
        sender.start();

        forward(1L);

        verify(publication, timeout(2000).times(3)).offer(any(DirectBuffer.class), anyInt(), anyInt());
        assertThat(sender.metrics().offerRetryCount()).isEqualTo(2);
    }

    private void forward(long orderId) {
        sender.forwardPlace(orderId, ACCOUNT_ID, STOCK_CODE, OrderSide.BUY, new BigDecimal("10000"), 1);
    }
}
