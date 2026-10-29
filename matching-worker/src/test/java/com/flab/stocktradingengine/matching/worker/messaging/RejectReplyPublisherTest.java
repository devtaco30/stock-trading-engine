package com.flab.stocktradingengine.matching.worker.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.AccountDestinationResolver;
import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.codec.OrderCodec;
import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;

/** 담당이 아니라 접수하지 않은 주문을 거부 회신 스트림(6003)으로 계좌 shard에 되돌리는 발신 통로를 확인한다. */
class RejectReplyPublisherTest {

    private static final String CHANNEL = "aeron:ipc";
    private static final String ENDPOINT = "endpoint-of-account-shard-a";

    private final OrderCodec codec = new OrderCodec();
    private final Map<Long, String> endpointByAccountId = new ConcurrentHashMap<>();
    private final AccountDestinationResolver resolver = new AccountDestinationResolver() {
        @Override
        public Optional<String> orderEndpointFor(long accountId) {
            return Optional.empty();
        }

        @Override
        public Optional<String> fillEndpointFor(long accountId) {
            return Optional.ofNullable(endpointByAccountId.get(accountId));
        }
    };

    private MediaDriver driver;
    private Aeron aeron;
    private ExclusivePublication rejectPublication;
    private RejectReplyPublisher publisher;

    @BeforeEach
    void setUp() {
        driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
            .aeronDirectoryName(CommonContext.generateRandomDirName()).dirDeleteOnStart(true).dirDeleteOnShutdown(true));
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        rejectPublication = aeron.addExclusivePublication(CHANNEL, AeronStreamIds.REJECT_REPLY);
    }

    @AfterEach
    void tearDown() {
        if (publisher != null) {
            publisher.close();
        }
        aeron.close();
        driver.close();
    }

    @Test
    void 담당_아닌_주문을_새_스트림으로_되돌리고_체결_스트림에는_아무것도_보내지_않는다() throws Exception {
        Subscription rejectSubscription = aeron.addSubscription(CHANNEL, AeronStreamIds.REJECT_REPLY);
        Subscription fillSubscription = aeron.addSubscription(CHANNEL, AeronStreamIds.FILL);
        endpointByAccountId.put(101L, ENDPOINT);
        publisher = new RejectReplyPublisher(resolver, Map.of(ENDPOINT, rejectPublication));
        publisher.start();
        JournaledOrder order = place(1L, 101L);

        publisher.onNotOwned(order);

        List<JournaledOrder> received = poll(rejectSubscription, 1);
        assertThat(received).containsExactly(order);
        assertThat(poll(fillSubscription, 0)).isEmpty();
    }

    @Test
    void 목적지를_아직_모르면_보관했다가_알게_되면_들어온_순서대로_보낸다() throws Exception {
        Subscription rejectSubscription = aeron.addSubscription(CHANNEL, AeronStreamIds.REJECT_REPLY);
        publisher = new RejectReplyPublisher(resolver, Map.of(ENDPOINT, rejectPublication));
        publisher.start();
        JournaledOrder first = place(1L, 101L);
        JournaledOrder second = place(2L, 101L);
        JournaledOrder third = place(3L, 101L);

        publisher.onNotOwned(first);
        publisher.onNotOwned(second);
        Thread.sleep(200);
        assertThat(poll(rejectSubscription, 0)).isEmpty();
        endpointByAccountId.put(101L, ENDPOINT);
        publisher.onNotOwned(third);

        assertThat(poll(rejectSubscription, 3)).containsExactly(first, second, third);
    }

    @Test
    void 수신자가_아직_연결되지_않았으면_보관했다가_연결되면_보낸다() throws Exception {
        endpointByAccountId.put(101L, ENDPOINT);
        publisher = new RejectReplyPublisher(resolver, Map.of(ENDPOINT, rejectPublication));
        publisher.start();
        JournaledOrder order = place(5L, 101L);

        publisher.onNotOwned(order);
        Thread.sleep(200);
        Subscription rejectSubscription = aeron.addSubscription(CHANNEL, AeronStreamIds.REJECT_REPLY);

        assertThat(poll(rejectSubscription, 1)).containsExactly(order);
    }

    @Test
    void queue가_가득_차면_넣는_쪽이_기다리고_기다린_횟수와_queue_사용량_최댓값을_센다() throws Exception {
        endpointByAccountId.put(101L, ENDPOINT);
        publisher = new RejectReplyPublisher(resolver, Map.of(ENDPOINT, rejectPublication), 4);
        Thread producer = new Thread(() -> {
            for (long orderId = 1; orderId <= 8; orderId++) {
                publisher.onNotOwned(place(orderId, 101L));
            }
        });

        producer.start();
        Thread.sleep(300);
        assertThat(producer.isAlive()).as("발신 thread가 아직 없어 queue가 차면 넣는 쪽이 기다려야 한다").isTrue();
        publisher.start();
        producer.join(3000);

        assertThat(producer.isAlive()).isFalse();
        assertThat(publisher.enqueueWaitCount()).isGreaterThan(0L);
        assertThat(publisher.enqueueWaitNanos()).isGreaterThan(0L);
        assertThat(publisher.maxQueueDepth()).isEqualTo(4);
    }

    @Test
    void 회신_코덱은_주문_내용을_그대로_되돌린다() {
        JournaledOrder order = place(9L, 777L);
        org.agrona.concurrent.UnsafeBuffer buffer = new org.agrona.concurrent.UnsafeBuffer(new byte[256]);

        int length = codec.encode(buffer, 0, order);

        assertThat(length).isPositive();
        assertThat(codec.decode(buffer, 0)).isEqualTo(order);
    }

    private JournaledOrder place(long orderId, long accountId) {
        return new JournaledOrder(EventType.PLACE, orderId, accountId, "005930", OrderSide.BUY,
            new BigDecimal("70000"), 10, Instant.parse("2026-10-29T00:00:00Z"), 11L, 22L, 33L);
    }

    /** 받은 주문을 디코딩해 모은다. expected개가 모일 때까지 최대 3초, 0이면 0.3초 기다린다. */
    private List<JournaledOrder> poll(Subscription subscription, int expected) throws InterruptedException {
        List<JournaledOrder> received = new ArrayList<>();
        long deadline = System.nanoTime() + (expected == 0 ? 300_000_000L : 3_000_000_000L);
        while (System.nanoTime() < deadline && (expected == 0 || received.size() < expected)) {
            subscription.poll((DirectBuffer buffer, int offset, int length, io.aeron.logbuffer.Header header) ->
                received.add(codec.decode(buffer, offset)), 10);
            Thread.sleep(2);
        }
        return received;
    }
}
