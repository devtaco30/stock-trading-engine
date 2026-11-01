package com.flab.stocktradingengine.matching.worker.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.flab.stocktradingengine.codec.FillCodec;
import com.flab.stocktradingengine.codec.FilledTrade;

import io.aeron.ExclusivePublication;
import io.aeron.Publication;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

/**
 * fork3, I4 U3 — 종료 드레인(D4)과 큐 가득참 경로(D2)를 검증한다. 목적지 endpoint 하나를 상대하는
 * {@link FillOutbox}를 mock {@link ExclusivePublication}과 직접 구성해, 라우팅·fan-out과 무관하게
 * 이 클래스의 종료·배압 동작만 본다.
 */
class FillOutboxTest {

    private static final String ENDPOINT = "aeron:udp?endpoint=localhost:6001";
    private final FillCodec codec = new FillCodec();

    @Test
    void 종료_시_큐에_남은_체결을_전부_드레인한다() {
        ExclusivePublication publication = mock(ExclusivePublication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(100L);
        FillOutbox outbox = new FillOutbox(ENDPOINT, publication, codec);

        for (int i = 0; i < 5; i++) {
            outbox.enqueueNeverDrop(sampleTrade(i));
        }
        outbox.start();
        outbox.close(2000);

        verify(publication, times(5)).offer(any(DirectBuffer.class), anyInt(), anyInt());
    }

    @Test
    void 드레인이_타임아웃_안에_못_끝나면_처리_중이던_것까지_포함해_남은_개수를_ERROR로_남긴다() {
        // 첫 체결이 발신 재시도에 영원히 갇혀 큐에서는 이미 빠졌지만(poll은 됐음) 아직 나가지
        // 못한 상태를 만든다 — 이 "처리 중" 1건도 드레인 실패로 잡혀야 한다(큐 안에 있는 것만
        // 세면 이 경우 큐가 비어 있어 0건으로 보인다).
        ExclusivePublication publication = mock(ExclusivePublication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(Publication.BACK_PRESSURED);
        FillOutbox outbox = new FillOutbox(ENDPOINT, publication, codec);
        outbox.enqueueNeverDrop(sampleTrade(1));
        outbox.start();

        Logger fillOutboxLogger = (Logger) LoggerFactory.getLogger(FillOutbox.class);
        ListAppender<ILoggingEvent> logAppender = new ListAppender<>();
        logAppender.start();
        fillOutboxLogger.addAppender(logAppender);

        try {
            outbox.close(200);
        } finally {
            fillOutboxLogger.detachAppender(logAppender);
        }

        assertThat(logAppender.list)
            .extracting(ILoggingEvent::getFormattedMessage)
            .anyMatch(message -> message.contains("1건"));
    }

    @Test
    void 큐가_가득_차면_버리지_않고_기다렸다가_공간이_생기면_이어서_보낸다() throws InterruptedException {
        ExclusivePublication publication = mock(ExclusivePublication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(100L);
        FillOutbox outbox = new FillOutbox(ENDPOINT, publication, codec);

        // start()를 아직 안 불러 아무도 큐를 비우지 않는다 — 용량만큼 채운다.
        for (int i = 0; i < FillOutbox.QUEUE_CAPACITY; i++) {
            outbox.enqueueNeverDrop(sampleTrade(i));
        }

        Thread blockedProducer = new Thread(() -> outbox.enqueueNeverDrop(sampleTrade(999_999)));
        blockedProducer.setDaemon(true);
        blockedProducer.start();
        blockedProducer.join(300);
        assertThat(blockedProducer.isAlive()).isTrue(); // 버리지 않고 그 자리에서 기다리는 중

        outbox.start(); // 드레인 시작 — 공간이 생기면 대기하던 enqueue가 끝나야 한다
        blockedProducer.join(2000);
        assertThat(blockedProducer.isAlive()).isFalse();

        outbox.close(5000);
        verify(publication, times(FillOutbox.QUEUE_CAPACITY + 1)).offer(any(DirectBuffer.class), anyInt(), anyInt());
    }

    @Test
    void 큐에_쌓인_개수와_가득_차_기다린_횟수와_시간을_읽을_수_있다() throws InterruptedException {
        ExclusivePublication publication = mock(ExclusivePublication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(100L);
        FillOutbox outbox = new FillOutbox(ENDPOINT, publication, codec);
        assertThat(outbox.queueDepth()).isZero();
        assertThat(outbox.enqueueWaitCount()).isZero();
        for (int i = 0; i < FillOutbox.QUEUE_CAPACITY; i++) {
            outbox.enqueueNeverDrop(sampleTrade(i));
        }
        assertThat(outbox.queueDepth()).as("소비하는 쪽이 없는 동안 쌓인 개수").isEqualTo(FillOutbox.QUEUE_CAPACITY);
        assertThat(outbox.enqueueWaitCount()).as("가득 차기 전에는 기다리지 않았다").isZero();

        Thread blockedProducer = new Thread(() -> outbox.enqueueNeverDrop(sampleTrade(999_999)));
        blockedProducer.setDaemon(true);
        blockedProducer.start();
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (outbox.enqueueWaitCount() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(outbox.enqueueWaitCount()).as("가득 찬 큐에 넣으려고 기다리기 시작한 횟수").isEqualTo(1);

        assertThat(outbox.currentEnqueueWaitNanos()).as("기다리는 중에는 진행 중인 대기 시간이 보인다").isPositive();

        outbox.start();
        blockedProducer.join(3000);
        outbox.close(5000);

        assertThat(blockedProducer.isAlive()).isFalse();
        assertThat(outbox.enqueueWaitNanos()).as("기다린 총 시간").isPositive();
        assertThat(outbox.queueDepth()).isZero();
        assertThat(outbox.currentEnqueueWaitNanos()).as("기다림이 끝나면 진행 중인 대기는 없다").isZero();
    }

    private FilledTrade sampleTrade(long seed) {
        return new FilledTrade(seed, "005930", seed, 1L, seed, 2L, 4, new BigDecimal("10000"));
    }
}
