package com.flab.stocktradingengine.matching.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.matching.worker.MatchingWorkerApplication;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.FillCodec;
import com.flab.stocktradingengine.codec.FilledTrade;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.codec.OrderCodec;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;

/**
 * matching-worker 앱을 실제로 띄우고, 진짜 Aeron IPC로 교차 주문을 발신해 매칭까지 되는지
 * 확인하는 end-to-end 테스트(파이프라인 연결 ①). {@link MatchingOrderIntakeConfig}가 만든
 * MediaDriver·Aeron을 그대로 쓴다 — 같은 프로세스라 Aeron 빈을 재사용해 테스트용 Publication만
 * 새로 연다(account-worker {@code AccountOrderIntakeIntegrationTest}와 같은 결).
 *
 * <p>관측은 {@link MatchListener} 빈을 따로 추가하지 않고(프로덕션 {@code AccountFillPublisher}가
 * 유일한 MatchListener 빈이라는 전제를 지킨다), 이 테스트가 체결 스트림({@link MatchingFillPublishConfig})에
 * 직접 {@link Subscription}을 붙여 실제로 체결 하나가 도착하는지로 확인한다(fork3 U2 —
 * 매칭측 Archive 녹화가 제거돼 더 이상 recording position으로는 관측할 수 없다. 계좌측 REMOTE
 * 녹화는 U3에서 다룬다). shard-routing 미설정 폴백(슬롯 1개, {@link ShardRoutingConfig#DEFAULT_FILL_CHANNEL})
 * 이라 매수·매도 fan-out이 같은 목적지 1곳으로 합쳐진다.</p>
 *
 * <p>{@code @DirtiesContext} — 이 컨텍스트가 만든 임베디드 MediaDriver를 다음 테스트와 안 겹치게 한다.</p>
 */
@SpringBootTest(classes = MatchingWorkerApplication.class)
@DirtiesContext
class MatchingOrderIntakeIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;
    private static final int FRAGMENT_LIMIT = 10;

    private final OrderCodec orderCodec = new OrderCodec();
    private final FillCodec fillCodec = new FillCodec();

    @Autowired
    private Aeron aeron;

    @Test
    void Aeron_IPC로_들어온_교차_주문이_매칭돼_체결_스트림에_발행된다() throws Exception {
        long runId = System.nanoTime();
        long buyAccountId = runId;
        long sellAccountId = runId + 1;
        long buyOrderId = runId + 2;
        long sellOrderId = runId + 3;

        try (Subscription fillSubscription = aeron.addSubscription(ShardRoutingConfig.DEFAULT_FILL_CHANNEL, AeronStreamIds.FILL)) {
            awaitConnected(fillSubscription);

            Publication publication =
                aeron.addPublication(MatchingOrderIntakeConfig.DEFAULT_INTAKE_CHANNEL, AeronStreamIds.MATCHING_INTAKE);
            try {
                awaitConnected(publication);

                Instant now = Instant.now();
                send(publication, new JournaledOrder(
                    EventType.PLACE, buyOrderId, buyAccountId, STOCK, OrderSide.BUY, new BigDecimal("10000"), 4, now));
                send(publication, new JournaledOrder(
                    EventType.PLACE, sellOrderId, sellAccountId, STOCK, OrderSide.SELL, new BigDecimal("10000"), 4, now.plusMillis(1)));

                FilledTrade trade = awaitFill(fillSubscription);
                assertThat(trade.buyAccountId()).isEqualTo(buyAccountId);
                assertThat(trade.sellAccountId()).isEqualTo(sellAccountId);
                assertThat(trade.filledQuantity()).isEqualTo(4);
            } finally {
                publication.close();
            }
        }
    }

    private FilledTrade awaitFill(Subscription subscription) {
        List<FilledTrade> received = new ArrayList<>();
        FragmentHandler handler = (buffer, offset, length, header) -> received.add(fillCodec.decode(buffer, offset));

        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (received.isEmpty()) {
            subscription.poll(handler, FRAGMENT_LIMIT);
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 체결 스트림에 체결이 발행되지 않음 — 매칭이 안 된 것으로 보임");
            }
        }
        return received.get(0);
    }

    /** 주문을 인코딩해 Aeron으로 발행한다. 백프레셔면 받아들여질 때까지 재시도한다. */
    private void send(Publication publication, JournaledOrder order) {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        int length = orderCodec.encode(buffer, 0, order);

        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        long result;
        do {
            result = publication.offer(buffer, 0, length);
            if (result > 0) {
                return;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 주문 발행 실패 — offer 반환=" + result);
            }
            Thread.yield();
        } while (true);
    }

    private void awaitConnected(Publication publication) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (!publication.isConnected()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 Publication이 연결되지 않음");
            }
            Thread.yield();
        }
    }

    private void awaitConnected(Subscription subscription) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (!subscription.isConnected()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 체결 구독이 연결되지 않음");
            }
            Thread.yield();
        }
    }
}
