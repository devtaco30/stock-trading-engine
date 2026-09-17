package com.flab.stocktradingengine.account.worker.messaging;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import com.flab.stocktradingengine.account.worker.recovery.OrderResultForwardPositionStore;
import com.flab.stocktradingengine.codec.OrderResultCodec;
import com.flab.stocktradingengine.codec.OrderResultEntry;
import com.flab.stocktradingengine.codec.OrderVerdict;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;

/**
 * U3-b — {@code OrderResultForwarder}가 실제 Aeron 전달(Header 포함)로 라이브 폴 루프를 돌 때,
 * {@code POSITION_SAVE_INTERVAL}(100)건마다 한 번 position을 저장하는지 검증한다. 이 테스트만
 * 실제 Subscription으로 {@code start()}를 돌린다 — {@link OrderResultForwarderTest}는 header가
 * 필요 없는 매핑·skip 로직만 보므로 {@code onFragment}를 직접 부른다.
 */
class OrderResultForwarderPositionSaveTest {

    private static final String CHANNEL = "aeron:ipc";
    private static final int STREAM_ID = 9201;
    private static final long TIMEOUT_NANOS = 5_000_000_000L;
    private static final long RECORDING_ID = 42L;
    private static final OrderResultCodec CODEC = new OrderResultCodec();

    private MediaDriver driver;
    private Aeron aeron;
    private ExclusivePublication publication;
    private Subscription subscription;
    private OrderResultForwarder forwarder;

    @BeforeEach
    void setUp() {
        driver = MediaDriver.launchEmbedded(new MediaDriver.Context().dirDeleteOnStart(true).dirDeleteOnShutdown(true));
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        publication = aeron.addExclusivePublication(CHANNEL, STREAM_ID);
        subscription = aeron.addSubscription(CHANNEL, STREAM_ID);
        awaitConnected();
    }

    @AfterEach
    void tearDown() {
        if (forwarder != null) {
            forwarder.close();
        }
        subscription.close();
        publication.close();
        aeron.close();
        driver.close();
    }

    @Test
    @DisplayName("POSITION_SAVE_INTERVAL(100)건마다 한 번 (recordingId, position)을 저장한다")
    void N건마다_한번_position을_저장한다() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send(any(String.class), any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        OrderResultForwardPositionStore positionStore = mock(OrderResultForwardPositionStore.class);
        forwarder = new OrderResultForwarder(subscription, kafkaTemplate, positionStore, RECORDING_ID);
        forwarder.start();

        int entriesToSend = 250; // POSITION_SAVE_INTERVAL(100)의 두 배를 넘겨 최소 두 번 저장돼야 한다
        for (int i = 0; i < entriesToSend; i++) {
            offer(new OrderResultEntry(1L, i, "r" + i, OrderVerdict.ACCEPTED, null, 1_000L + i));
        }

        verify(positionStore, timeout(5000).times(2)).write(eq(RECORDING_ID), anyLong());
    }

    private void offer(OrderResultEntry entry) {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        int length = CODEC.encode(buffer, 0, entry);
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        long result;
        do {
            result = publication.offer(buffer, 0, length);
            if (result > 0) {
                return;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 엔트리 발행 실패 — offer 반환=" + result);
            }
            Thread.yield();
        } while (true);
    }

    private void awaitConnected() {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (!publication.isConnected()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 Publication이 연결되지 않음");
            }
            Thread.yield();
        }
    }
}
