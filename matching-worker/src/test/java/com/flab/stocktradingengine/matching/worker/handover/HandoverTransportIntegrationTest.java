package com.flab.stocktradingengine.matching.worker.handover;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.aeron.MatchingSlotTarget;
import com.flab.stocktradingengine.matching.disruptor.handover.HandoverMessage;
import com.flab.stocktradingengine.matching.worker.coordination.HandoverEndpointPool;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;

/**
 * 호가창 이동의 Aeron 전송(U15a). 한 JVM 안의 임베디드 MediaDriver 하나에 매칭 pod 둘(이전 담당 X, 새 담당 Z)을 UDP localhost로
 * 붙인다. Z가 X의 주소로 요청(2008)을 보내고, X가 Z의 주소로 응답(2009)을 종목마다 메시지 하나로 보내면 Z가 조립해 적재 요청을 낸다.
 */
class HandoverTransportIntegrationTest {

    private static final String X_CHANNEL = "aeron:udp?endpoint=localhost:20730";
    private static final String Z_CHANNEL = "aeron:udp?endpoint=localhost:20731";
    private static final int SLOT = 12;
    private static final long TIMEOUT_NANOS = 8_000_000_000L;

    private MediaDriver driver;
    private Aeron aeron;
    private HandoverBookStore xStore;
    private HandoverResponder xResponder;
    private HandoverRequester zRequester;
    private HandoverReceiver zReceiver;
    private final List<Assembled> assembled = new CopyOnWriteArrayList<>();
    private final AtomicReference<Optional<MatchingSlotTarget>> zMapEntry =
        new AtomicReference<>(Optional.of(new MatchingSlotTarget(X_CHANNEL, true)));

    private record Assembled(long transferId, int stockSlot, List<byte[]> bodies) {
    }

    @BeforeEach
    void setUp() {
        driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
            .dirDeleteOnStart(true).dirDeleteOnShutdown(true).threadingMode(ThreadingMode.SHARED));
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        List<String> poolEndpoints = List.of(X_CHANNEL, Z_CHANNEL);

        xStore = new HandoverBookStore();
        xResponder = new HandoverResponder(aeron, X_CHANNEL, new HandoverEndpointPool(poolEndpoints, X_CHANNEL), xStore);

        IntFunction<Optional<MatchingSlotTarget>> zMap = slot -> zMapEntry.get();
        HandoverEndpointPool zPool = new HandoverEndpointPool(poolEndpoints, Z_CHANNEL);
        zRequester = new HandoverRequester(aeron, Z_CHANNEL, zPool, zMap, 6L);
        zReceiver = new HandoverReceiver(aeron, Z_CHANNEL, zRequester,
            (transferId, stockSlot, bodies) -> assembled.add(new Assembled(transferId, stockSlot, bodies)));
        xResponder.start();
        zReceiver.start();
    }

    @AfterEach
    void tearDown() {
        zReceiver.close();
        xResponder.close();
        zRequester.close();
        aeron.close();
        driver.close();
    }

    @Test
    void Z가_요청하면_X가_보관한_종목별_바이트를_보내고_Z가_조립해_적재_요청을_낸다() {
        xStore.put(SLOT, List.of(bytes(1, 300), bytes(2, 5_000), bytes(3, 0)));

        long transferId = zRequester.request(SLOT).orElseThrow();
        awaitAssembled(1);

        Assembled result = assembled.get(0);
        assertThat(result.transferId()).isEqualTo(transferId);
        assertThat(result.stockSlot()).isEqualTo(SLOT);
        assertThat(result.bodies()).hasSize(3);
        assertThat(result.bodies().get(0)).isEqualTo(bytes(1, 300));
        assertThat(result.bodies().get(1)).isEqualTo(bytes(2, 5_000));
        assertThat(result.bodies().get(2)).isEqualTo(bytes(3, 0));
    }

    @Test
    void 재요청은_같은_이동_번호로_나가고_같은_이동의_중복_응답은_한_번만_적재한다() throws Exception {
        xStore.put(SLOT, List.of(bytes(1, 300), bytes(2, 400)));

        long transferId = zRequester.request(SLOT).orElseThrow();
        assertThat(zRequester.resend(SLOT)).isTrue();
        assertThat(zRequester.resend(SLOT)).isTrue();
        awaitAssembled(1);
        Thread.sleep(400); // 재요청마다 X가 같은 바이트를 다시 보낸다. 늦게 온 중복 응답이 모두 도착할 시간을 준다.

        assertThat(assembled).as("같은 이동의 중복 응답은 먼저 완성된 것 하나만 적재한다").hasSize(1);
        assertThat(assembled.get(0).transferId()).isEqualTo(transferId);
        assertThat(assembled.get(0).bodies().get(0)).isEqualTo(bytes(1, 300));
    }

    @Test
    void 포기해서_취소한_뒤_늦게_온_응답은_버려진다() throws Exception {
        xStore.put(SLOT, List.of(bytes(1, 300)));

        zRequester.request(SLOT).orElseThrow();
        zRequester.cancel(SLOT); // 포기 시간을 넘겨 실패로 끝낸 것과 같다
        awaitUntil(() -> zReceiver.discardedMessages() >= 1);

        assertThat(assembled).isEmpty();
        assertThat(zRequester.inFlight(SLOT)).isFalse();
        assertThat(zRequester.resend(SLOT)).as("취소한 요청은 다시 보내지 않는다").isFalse();
    }

    @Test
    void 보낼_종목이_하나도_없는_종목_slot도_빈_목록으로_완료된다() {
        xStore.put(SLOT, List.of());

        zRequester.request(SLOT).orElseThrow();
        awaitAssembled(1);

        assertThat(assembled.get(0).bodies()).isEmpty();
    }

    @Test
    void 맵에_이동_중_항목이_없으면_요청하지_않는다() {
        zMapEntry.set(Optional.empty());
        assertThat(zRequester.request(SLOT)).isEmpty();

        zMapEntry.set(Optional.of(new MatchingSlotTarget(X_CHANNEL, false)));
        assertThat(zRequester.request(SLOT)).isEmpty();
    }

    @Test
    void 이전_담당_주소가_풀에_없으면_바로_실패한다() {
        zMapEntry.set(Optional.of(new MatchingSlotTarget("aeron:udp?endpoint=localhost:29999", true)));

        assertThatThrownBy(() -> zRequester.request(SLOT))
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("풀에 없는 주소");
    }

    @Test
    void 현재_요청과_전송_id가_다른_응답은_버리고_맞는_응답만_적재한다() {
        xStore.put(SLOT, List.of(bytes(7, 40)));
        long staleTransferId = (6L << 53) | 999L;
        Publication toZ = aeron.addPublication(Z_CHANNEL, AeronStreamIds.ORDER_BOOK_RESPONSE);
        awaitConnected(toZ);
        send(toZ, new HandoverMessage(staleTransferId, SLOT, 0, 1, bytes(9, 10)).encode());

        long transferId = zRequester.request(SLOT).orElseThrow();
        awaitAssembled(1);

        assertThat(assembled).hasSize(1);
        assertThat(assembled.get(0).transferId()).isEqualTo(transferId);
        assertThat(assembled.get(0).bodies().get(0)).isEqualTo(bytes(7, 40));
        assertThat(zReceiver.discardedMessages()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void 깨진_요청_하나는_그_요청만_포기하고_응답_thread는_다음_요청을_계속_처리한다() {
        Publication toX = aeron.addPublication(X_CHANNEL, AeronStreamIds.ORDER_BOOK_REQUEST);
        awaitConnected(toX);
        xStore.put(SLOT, List.of(bytes(1, 10)));

        send(toX, new byte[] {1, 2, 3});
        awaitUntil(() -> xResponder.requestFailures() >= 1);
        zRequester.request(SLOT).orElseThrow();
        awaitAssembled(1);

        assertThat(xResponder.requestFailures()).isEqualTo(1);
        assertThat(assembled.get(0).bodies().get(0)).isEqualTo(bytes(1, 10));
    }

    @Test
    void 깨진_응답_하나는_그_메시지만_포기하고_수신_thread는_다음_응답을_계속_처리한다() {
        xStore.put(SLOT, List.of(bytes(5, 10)));
        Publication toZ = aeron.addPublication(Z_CHANNEL, AeronStreamIds.ORDER_BOOK_RESPONSE);
        awaitConnected(toZ);

        send(toZ, new byte[] {1, 2, 3});
        awaitUntil(() -> zReceiver.messageFailures() >= 1);
        zRequester.request(SLOT).orElseThrow();
        awaitAssembled(1);

        assertThat(zReceiver.messageFailures()).isEqualTo(1);
        assertThat(assembled.get(0).bodies().get(0)).isEqualTo(bytes(5, 10));
    }

    @Test
    void X에_보관한_바이트가_없으면_응답하지_않고_세어_둔다() {
        zRequester.request(SLOT).orElseThrow();

        awaitUntil(() -> xResponder.requestsWithoutBook() >= 1);

        assertThat(assembled).isEmpty();
    }

    @Test
    void 응답_주소가_풀에_없는_요청은_응답하지_않고_세어_둔다() {
        Publication toX = aeron.addPublication(X_CHANNEL, AeronStreamIds.ORDER_BOOK_REQUEST);
        awaitConnected(toX);
        xStore.put(SLOT, List.of(bytes(1, 10)));

        send(toX, new HandoverRequest(1L, SLOT, "aeron:udp?endpoint=localhost:29998").encode());

        awaitUntil(() -> xResponder.requestsRejectedByPool() >= 1);
        assertThat(assembled).isEmpty();
    }

    private void awaitAssembled(int count) {
        awaitUntil(() -> assembled.size() >= count);
    }

    private static void awaitUntil(java.util.function.BooleanSupplier condition) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("8초 안에 조건이 충족되지 않음");
            }
            Thread.onSpinWait();
        }
    }

    private static void awaitConnected(Publication publication) {
        awaitUntil(publication::isConnected);
    }

    private static void send(Publication publication, byte[] bytes) {
        UnsafeBuffer buffer = new UnsafeBuffer(bytes);
        awaitUntil(() -> publication.offer(buffer, 0, bytes.length) > 0);
    }

    private static byte[] bytes(int seed, int length) {
        byte[] out = new byte[length];
        for (int i = 0; i < length; i++) {
            out[i] = (byte) (seed * 31 + i);
        }
        return out;
    }
}
