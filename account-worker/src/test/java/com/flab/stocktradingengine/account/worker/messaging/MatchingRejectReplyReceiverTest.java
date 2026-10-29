package com.flab.stocktradingengine.account.worker.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.DatagramSocket;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.codec.OrderCodec;
import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;

/**
 * 매칭 샤딩 U9d(계좌 쪽) — 매칭 pod가 담당이 아니라며 돌려보낸 주문(스트림 6003)을 읽어
 * {@link AeronMatchingOrderSender#acceptRejectedOrder}로 넘기는 수신기. 체결 스트림(6001)과 같은
 * 주소를 쓰되 스트림 번호만 다르므로 서로 섞여 읽히지 않아야 한다.
 */
class MatchingRejectReplyReceiverTest {

    private static final String STOCK_CODE = "005930";
    private static final long AWAIT_MILLIS = 3000L;

    private final OrderCodec codec = new OrderCodec();
    private final List<JournaledOrder> received = new CopyOnWriteArrayList<>();

    private MatchingRejectReplyReceiver receiver;
    private MediaDriver mediaDriver;
    private Aeron aeron;

    @AfterEach
    void tearDown() {
        CloseHelper.quietClose(receiver);
        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(mediaDriver);
    }

    @Test
    void 디코딩된_주문을_거부_회신_입구에_넘긴다() {
        receiver = new MatchingRejectReplyReceiver(null, received::add);
        UnsafeBuffer buffer = encoded(order(11L));

        receiver.onFragment(buffer, 0, buffer.capacity(), null);

        assertThat(received).containsExactly(order(11L));
    }

    @Test
    void 깨진_바이트는_넘기지_않고_예외도_던지지_않는다() {
        receiver = new MatchingRejectReplyReceiver(null, received::add);
        UnsafeBuffer garbage = new UnsafeBuffer(new byte[] {(byte) 0x7f, 1, 2});

        receiver.onFragment(garbage, 0, garbage.capacity(), null);

        assertThat(received).isEmpty();
    }

    @Test
    void 같은_주소의_체결_스트림_메시지는_읽지_않고_거부_회신_스트림만_읽는다() throws Exception {
        String channel = "aeron:udp?endpoint=localhost:" + freePort();
        mediaDriver = MediaDriver.launchEmbedded(new MediaDriver.Context()
            .aeronDirectoryName(CommonContext.generateRandomDirName())
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED));
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
        Subscription rejectSubscription = aeron.addSubscription(channel, AeronStreamIds.REJECT_REPLY);
        // 체결 스트림에 구독자가 있어야 그 Publication이 연결된다(이 구독은 수신기와 무관한 별개 구독이다).
        Subscription fillSubscription = aeron.addSubscription(channel, AeronStreamIds.FILL);
        Publication rejectPublication = aeron.addPublication(channel, AeronStreamIds.REJECT_REPLY);
        Publication fillPublication = aeron.addPublication(channel, AeronStreamIds.FILL);
        receiver = new MatchingRejectReplyReceiver(rejectSubscription, received::add);
        receiver.start();
        awaitConnected(rejectPublication);
        awaitConnected(fillPublication);

        offer(fillPublication, encoded(order(1L)));
        offer(rejectPublication, encoded(order(2L)));

        long deadline = System.currentTimeMillis() + AWAIT_MILLIS;
        while (received.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        Thread.sleep(200);
        assertThat(received).containsExactly(order(2L));
        assertThat(fillSubscription.isConnected()).isTrue();
    }

    private JournaledOrder order(long orderId) {
        return new JournaledOrder(EventType.PLACE, orderId, 1L, STOCK_CODE, OrderSide.BUY,
            new BigDecimal("10000"), 10, Instant.ofEpochSecond(1_700_000_000L));
    }

    private UnsafeBuffer encoded(JournaledOrder order) {
        UnsafeBuffer scratch = new UnsafeBuffer(new byte[256]);
        int length = codec.encode(scratch, 0, order);
        UnsafeBuffer exact = new UnsafeBuffer(new byte[length]);
        exact.putBytes(0, scratch, 0, length);
        return exact;
    }

    private void offer(Publication publication, UnsafeBuffer buffer) throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_MILLIS;
        while (publication.offer(buffer, 0, buffer.capacity()) < 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
    }

    private void awaitConnected(Publication publication) throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_MILLIS;
        while (!publication.isConnected() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertThat(publication.isConnected()).isTrue();
    }

    private static int freePort() throws IOException {
        try (DatagramSocket socket = new DatagramSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
