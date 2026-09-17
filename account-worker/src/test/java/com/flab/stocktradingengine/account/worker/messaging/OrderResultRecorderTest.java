package com.flab.stocktradingengine.account.worker.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.account.disruptor.domain.RejectReason;
import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.codec.OrderResultCodec;
import com.flab.stocktradingengine.codec.OrderResultEntry;
import com.flab.stocktradingengine.codec.OrderVerdict;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;

/**
 * U2 — {@link OrderResultRecorder}가 네 콜백에서 기대한 엔트리를 결과 스트림에 실제로 내보내는지
 * Aeron IPC로 검증한다(Archive 녹화는 여기서 다루지 않는다 — 발행 배선만 확인).
 */
class OrderResultRecorderTest {

    private static final String CHANNEL = "aeron:ipc";
    private static final long TIMEOUT_NANOS = 5_000_000_000L; // 5초

    private final OrderResultCodec codec = new OrderResultCodec();

    private MediaDriver driver;
    private Aeron aeron;
    private ExclusivePublication publication;
    private Subscription subscription;
    private OrderResultRecorder recorder;

    @BeforeEach
    void 발행자와_구독자를_연결한다() {
        driver = MediaDriver.launchEmbedded(new MediaDriver.Context().dirDeleteOnStart(true).dirDeleteOnShutdown(true));
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        publication = aeron.addExclusivePublication(CHANNEL, AeronStreamIds.ORDER_RESULT);
        subscription = aeron.addSubscription(CHANNEL, AeronStreamIds.ORDER_RESULT);
        recorder = new OrderResultRecorder(publication);
        awaitConnected();
    }

    @AfterEach
    void 정리한다() {
        subscription.close();
        publication.close();
        aeron.close();
        driver.close();
    }

    @Test
    @DisplayName("onAccepted — ACCEPTED, rejectReason 없음")
    void onAccepted_ACCEPTED_기록() {
        recorder.onAccepted(1L, 100L, "r1", new BigDecimal("500.00"));

        OrderResultEntry entry = pollForEntry();

        assertEquals(1L, entry.accountId());
        assertEquals(100L, entry.orderId());
        assertEquals("r1", entry.requestId());
        assertEquals(OrderVerdict.ACCEPTED, entry.verdict());
        assertNull(entry.rejectReason());
    }

    @Test
    @DisplayName("onSellAccepted — ACCEPTED, rejectReason 없음")
    void onSellAccepted_ACCEPTED_기록() {
        recorder.onSellAccepted(2L, 200L, "r2", 10);

        OrderResultEntry entry = pollForEntry();

        assertEquals(2L, entry.accountId());
        assertEquals(200L, entry.orderId());
        assertEquals(OrderVerdict.ACCEPTED, entry.verdict());
        assertNull(entry.rejectReason());
    }

    @Test
    @DisplayName("onRejected — REJECTED, 거부 사유가 실린다")
    void onRejected_REJECTED_거부사유_기록() {
        recorder.onRejected(3L, 0L, "r3", RejectReason.INSUFFICIENT);

        OrderResultEntry entry = pollForEntry();

        assertEquals(3L, entry.accountId());
        assertEquals(0L, entry.orderId());
        assertEquals(OrderVerdict.REJECTED, entry.verdict());
        assertEquals("INSUFFICIENT", entry.rejectReason());
    }

    @Test
    @DisplayName("onDuplicateRequest — DUPLICATE, orderId=0, rejectReason 없음")
    void onDuplicateRequest_DUPLICATE_기록() {
        recorder.onDuplicateRequest(4L, "r4");

        OrderResultEntry entry = pollForEntry();

        assertEquals(4L, entry.accountId());
        assertEquals(0L, entry.orderId());
        assertEquals("r4", entry.requestId());
        assertEquals(OrderVerdict.DUPLICATE, entry.verdict());
        assertNull(entry.rejectReason());
    }

    private void awaitConnected() {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (!publication.isConnected()) {
            if (System.nanoTime() > deadline) {
                fail("5초 안에 Publication이 연결되지 않음");
            }
            Thread.yield();
        }
    }

    private OrderResultEntry pollForEntry() {
        AtomicReference<OrderResultEntry> received = new AtomicReference<>();
        FragmentHandler handler = (buffer, offset, length, header) -> {
            Optional<OrderResultEntry> decoded = codec.tryDecode(buffer, offset, length);
            decoded.ifPresent(received::set);
        };

        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (received.get() == null) {
            int fragments = subscription.poll(handler, 10);
            if (fragments == 0) {
                if (System.nanoTime() > deadline) {
                    fail("5초 안에 결과 엔트리를 수신하지 못함");
                }
                Thread.yield();
            }
        }
        assertTrue(received.get() != null);
        return received.get();
    }
}
