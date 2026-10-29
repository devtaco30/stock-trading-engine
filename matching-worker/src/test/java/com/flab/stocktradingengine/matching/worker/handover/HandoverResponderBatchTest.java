package com.flab.stocktradingengine.matching.worker.handover;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.matching.disruptor.handover.HandoverMessage;
import com.flab.stocktradingengine.matching.worker.coordination.HandoverEndpointPool;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;

/**
 * 이전 담당(X)의 응답 thread가 한 번의 poll에서 같은 종목 slot의 요청을 여럿 읽었을 때 마지막 요청에만 응답하는지, 보관이 없어 응답하지
 * 못한 경우를 정리 뒤 늦게 온 중복 요청과 실제 보관 없음으로 나누어 세는지. 응답 thread는 시작하지 않고 시험이 poll을 직접 한 번 부른다.
 */
class HandoverResponderBatchTest {

    private static final String A_CHANNEL = "aeron:udp?endpoint=localhost:20740";
    private static final String Z_CHANNEL = "aeron:udp?endpoint=localhost:20741";
    private static final int SLOT = 5;

    private MediaDriver driver;
    private Aeron aeron;
    private HandoverBookStore store;
    private HandoverResponder responder;
    private Publication requestPublication;
    private Subscription zSubscription;

    @BeforeEach
    void setUp() {
        driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
            .dirDeleteOnStart(true).dirDeleteOnShutdown(true).threadingMode(ThreadingMode.SHARED));
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        store = new HandoverBookStore();
        responder = new HandoverResponder(aeron, A_CHANNEL, new HandoverEndpointPool(List.of(A_CHANNEL, Z_CHANNEL), A_CHANNEL), store);
        zSubscription = aeron.addSubscription(Z_CHANNEL, AeronStreamIds.ORDER_BOOK_RESPONSE);
        requestPublication = aeron.addPublication(A_CHANNEL, AeronStreamIds.ORDER_BOOK_REQUEST);
        awaitUntil(requestPublication::isConnected);
    }

    @AfterEach
    void tearDown() {
        responder.close();
        zSubscription.close();
        requestPublication.close();
        aeron.close();
        driver.close();
    }

    @Test
    void 한_번의_poll에서_같은_종목_slot의_요청_셋을_읽으면_마지막_요청에만_응답한다() throws Exception {
        store.put(SLOT, List.of(new byte[] {1, 2, 3}));
        sendRequest(101L, SLOT);
        sendRequest(102L, SLOT);
        sendRequest(103L, SLOT);
        Thread.sleep(300); // 셋 모두 구독에 도착해 한 번의 poll로 읽히게 한다

        responder.pollOnce();

        List<Long> answered = readResponses(500);
        assertThat(answered).as("옛 번호 요청에 호가창 전체를 보내도 받는 쪽이 버리므로 마지막 요청에만 응답한다").containsExactly(103L);
    }

    @Test
    void 다른_종목_slot의_요청은_각각_응답한다() throws Exception {
        store.put(SLOT, List.of(new byte[] {1}));
        store.put(SLOT + 1, List.of(new byte[] {2}));
        sendRequest(201L, SLOT);
        sendRequest(202L, SLOT + 1);
        Thread.sleep(300);

        responder.pollOnce();

        assertThat(readResponses(500)).containsExactlyInAnyOrder(201L, 202L);
    }

    @Test
    void 정리한_뒤_늦게_온_중복_요청과_실제_보관_없음을_나누어_센다() throws Exception {
        store.put(SLOT, List.of(new byte[] {1}));
        store.remove(SLOT); // 맵이 바뀌어 정리된 뒤
        sendRequest(301L, SLOT);
        sendRequest(302L, SLOT + 7); // 한 번도 보관한 적 없는 종목 slot
        Thread.sleep(300);

        responder.pollOnce();

        assertThat(responder.requestsAfterCleanup()).isEqualTo(1);
        assertThat(responder.requestsWithoutBook()).isEqualTo(1);
    }

    private void sendRequest(long transferId, int slot) {
        byte[] wire = new HandoverRequest(transferId, slot, Z_CHANNEL).encode();
        awaitUntil(() -> requestPublication.offer(new UnsafeBuffer(wire), 0, wire.length) > 0);
    }

    private List<Long> readResponses(long waitMillis) throws InterruptedException {
        List<Long> transferIds = new ArrayList<>();
        long deadline = System.nanoTime() + waitMillis * 1_000_000L;
        while (System.nanoTime() < deadline) {
            zSubscription.poll((buffer, offset, length, header) -> {
                byte[] wire = new byte[length];
                buffer.getBytes(offset, wire);
                transferIds.add(HandoverMessage.decode(wire, 0, length).transferId());
            }, 10);
            Thread.sleep(5);
        }
        return transferIds;
    }

    private static void awaitUntil(java.util.function.BooleanSupplier condition) {
        long deadline = System.nanoTime() + 8_000_000_000L;
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("8초 안에 조건이 충족되지 않음");
            }
            Thread.onSpinWait();
        }
    }
}
