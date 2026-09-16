package com.flab.stocktradingengine.account.worker.messaging;

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
import java.util.List;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.Publication;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

/**
 * I2(발신측) U1 — 안 버린다(D1)·전용 스레드가 재시도(D2)·복구 불가는 계좌 스레드로 전파(D3).
 * matching-worker {@code FillOutboxTest}·{@code AccountFillPublisherTest}(I4)와 같은 결로 검증한다.
 * 버퍼 재사용 테스트는 기존 것을 그대로 둔다(이번 변경과 무관).
 */
class AeronMatchingOrderSenderTest {

    private static final long ACCOUNT_ID = 1L;
    private static final String STOCK_CODE = "005930";
    private static final int VERIFY_TIMEOUT_MILLIS = 1000;

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
        verify(publication, timeout(VERIFY_TIMEOUT_MILLIS).times(2)).offer(bufferCaptor.capture(), anyInt(), anyInt());

        List<DirectBuffer> buffers = bufferCaptor.getAllValues();
        assertThat(buffers.get(0)).isSameAs(buffers.get(1));
    }

    @Test
    void 큐가_가득_차면_버리지_않고_기다렸다가_공간이_생기면_이어서_보낸다() throws InterruptedException {
        Publication publication = mock(Publication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(100L);
        sender = new AeronMatchingOrderSender(publication);

        // start()를 아직 안 불러 아무도 큐를 비우지 않는다 — 용량만큼 채운다.
        for (int i = 0; i < AeronMatchingOrderSender.OUTBOX_CAPACITY; i++) {
            sender.forwardPlace(i, ACCOUNT_ID, STOCK_CODE, OrderSide.BUY, new BigDecimal("10000"), 1);
        }

        Thread blockedProducer = new Thread(() ->
            sender.forwardPlace(999_999L, ACCOUNT_ID, STOCK_CODE, OrderSide.BUY, new BigDecimal("10000"), 1));
        blockedProducer.setDaemon(true);
        blockedProducer.start();
        blockedProducer.join(300);
        assertThat(blockedProducer.isAlive()).as("버리지 않고 그 자리에서 기다리는 중이어야 한다").isTrue();

        sender.start(); // 드레인 시작 — 공간이 생기면 대기하던 forwardPlace가 끝나야 한다
        blockedProducer.join(2000);
        assertThat(blockedProducer.isAlive()).isFalse();

        // 용량+1건(65537) 전부 처리되는 데는 BackoffIdleStrategy 사이클이 그만큼 반복돼야 해
        // 기본 검증 타임아웃(1초)로는 부하 상황에서 flaky하다 — 넉넉히 5초.
        verify(publication, timeout(5000).times(AeronMatchingOrderSender.OUTBOX_CAPACITY + 1))
            .offer(any(DirectBuffer.class), anyInt(), anyInt());
    }

    @Test
    void 스트림이_CLOSED면_재시도_없이_포기한다() {
        Publication publication = mock(Publication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(Publication.CLOSED);
        sender = new AeronMatchingOrderSender(publication);
        sender.start();

        sender.forwardPlace(1L, ACCOUNT_ID, STOCK_CODE, OrderSide.BUY, new BigDecimal("10000"), 10);

        // CLOSED는 복구 불가라 재시도 없이 즉시 포기한다 — offer는 정확히 한 번만 불린다.
        verify(publication, timeout(VERIFY_TIMEOUT_MILLIS).times(1)).offer(any(DirectBuffer.class), eq(0), anyInt());
    }

    @Test
    void 스트림이_복구_불가_상태면_다음_forwardPlace에서_계좌_스레드로_예외가_전파된다() {
        // 첫 forwardPlace는 publisher 스레드가 CLOSED를 아직 감지하기 전이라 큐잉만 하고 정상
        // 반환한다(비동기). 그 스레드가 CLOSED를 감지해 치명 상태를 기록한 뒤에는, 다음
        // forwardPlace가 계좌 소비자 스레드 자신에서 바로 예외를 던져야 AccountExceptionHandler가
        // 계좌 전체를 fail-fast로 멈출 수 있다.
        Publication publication = mock(Publication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(Publication.CLOSED);
        sender = new AeronMatchingOrderSender(publication);
        sender.start();

        sender.forwardPlace(1L, ACCOUNT_ID, STOCK_CODE, OrderSide.BUY, new BigDecimal("10000"), 10);
        verify(publication, timeout(VERIFY_TIMEOUT_MILLIS).times(1)).offer(any(DirectBuffer.class), eq(0), anyInt());

        assertThatThrownBy(() ->
            sender.forwardPlace(2L, ACCOUNT_ID, STOCK_CODE, OrderSide.BUY, new BigDecimal("10000"), 10))
            .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void 종료_시_큐에_남은_주문을_전부_드레인한다() {
        Publication publication = mock(Publication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(100L);
        sender = new AeronMatchingOrderSender(publication);

        for (int i = 0; i < 5; i++) {
            sender.forwardPlace(i, ACCOUNT_ID, STOCK_CODE, OrderSide.BUY, new BigDecimal("10000"), 1);
        }
        sender.start();
        sender.close(2000);

        verify(publication, times(5)).offer(any(DirectBuffer.class), anyInt(), anyInt());
    }

    @Test
    void 드레인이_타임아웃_안에_못_끝나면_처리_중이던_것까지_포함해_남은_개수를_ERROR로_남긴다() {
        // 첫 주문이 발신 재시도에 영원히 갇혀 큐에서는 이미 빠졌지만(poll은 됐음) 아직 나가지
        // 못한 상태를 만든다 — 이 "처리 중" 1건도 드레인 실패로 잡혀야 한다(큐 안에 있는 것만
        // 세면 이 경우 큐가 비어 있어 0건으로 보인다, I4 FillOutbox.close()가 이 자리에서 버그였다).
        Publication publication = mock(Publication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(Publication.BACK_PRESSURED);
        AeronMatchingOrderSender localSender = new AeronMatchingOrderSender(publication);
        localSender.forwardPlace(1L, ACCOUNT_ID, STOCK_CODE, OrderSide.BUY, new BigDecimal("10000"), 10);
        localSender.start();

        Logger senderLogger = (Logger) LoggerFactory.getLogger(AeronMatchingOrderSender.class);
        ListAppender<ILoggingEvent> logAppender = new ListAppender<>();
        logAppender.start();
        senderLogger.addAppender(logAppender);

        try {
            localSender.close(200);
        } finally {
            senderLogger.detachAppender(logAppender);
        }

        assertThat(logAppender.list)
            .extracting(ILoggingEvent::getFormattedMessage)
            .anyMatch(message -> message.contains("1건"));
    }
}
