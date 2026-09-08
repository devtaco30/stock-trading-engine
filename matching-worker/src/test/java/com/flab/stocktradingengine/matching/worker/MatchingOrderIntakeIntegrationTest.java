package com.flab.stocktradingengine.matching.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.agrona.concurrent.UnsafeBuffer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.test.annotation.DirtiesContext;

import com.flab.stocktradingengine.kafka.KafkaTopics;
import com.flab.stocktradingengine.kafka.event.TradeFilledEvent;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.wire.EventType;
import com.flab.stocktradingengine.wire.JournaledOrder;
import com.flab.stocktradingengine.wire.OrderCodec;

import io.aeron.Aeron;
import io.aeron.Publication;

/**
 * matching-worker 앱을 실제로 띄우고, 진짜 Aeron IPC로 교차 주문을 발신해 매칭까지 되는지
 * 확인하는 end-to-end 테스트(파이프라인 연결 ①). {@link MatchingOrderIntakeConfig}가 만든
 * MediaDriver·Aeron을 그대로 쓴다 — 같은 프로세스라 Aeron 빈을 재사용해 테스트용 Publication만
 * 새로 연다(account-worker {@code AccountOrderIntakeIntegrationTest}와 같은 결).
 *
 * <p>관측은 {@link MatchListener} 빈을 따로 추가하지 않고(프로덕션 {@code AccountFillPublisher}가
 * 유일한 MatchListener 빈이라는 전제를 지킨다), {@code MatchingFillIntegrationTest}와 같은 방식으로
 * 로컬 Kafka {@code account-fills} 토픽에 실제로 체결이 발행되는지로 확인한다.</p>
 *
 * <p>{@code @DirtiesContext} — 이 컨텍스트가 만든 임베디드 MediaDriver를 다음 테스트와 안 겹치게 한다.</p>
 */
@SpringBootTest(classes = MatchingWorkerApplication.class)
@DirtiesContext
class MatchingOrderIntakeIntegrationTest {

    private static final String STOCK = "005930";
    private static final String TOPIC = KafkaTopics.accountFills();
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;

    private final OrderCodec codec = new OrderCodec();

    @Autowired
    private Aeron aeron;

    @Test
    void Aeron_IPC로_들어온_교차_주문이_매칭돼_account_fills에_발행된다() throws Exception {
        // 계좌·주문 ID를 실행마다 새로 뽑는다 — 상수로 고정하면 이전 실행이 이 토픽에 남긴
        // 레코드까지 조건에 걸려서, 이번 실행이 실제로 매칭 안 해도 옛 레코드로 통과해버린다
        // (MatchingFillIntegrationTest와 같은 이유).
        long runId = System.nanoTime();
        long buyAccountId = runId;
        long sellAccountId = runId + 1;
        long buyOrderId = runId + 2;
        long sellOrderId = runId + 3;

        Publication publication =
            aeron.addPublication(MatchingOrderIntakeConfig.INTAKE_CHANNEL, MatchingOrderIntakeConfig.INTAKE_STREAM_ID);
        try {
            awaitConnected(publication);

            Instant now = Instant.now();
            send(publication, new JournaledOrder(
                EventType.PLACE, buyOrderId, buyAccountId, STOCK, OrderSide.BUY, new BigDecimal("10000"), 4, now));
            send(publication, new JournaledOrder(
                EventType.PLACE, sellOrderId, sellAccountId, STOCK, OrderSide.SELL, new BigDecimal("10000"), 4, now.plusMillis(1)));

            List<ConsumerRecord<String, TradeFilledEvent>> matched = consumeUntilBothSidesArrive(buyAccountId, sellAccountId);

            assertThat(matched).hasSize(2);
            TradeFilledEvent event = matched.get(0).value();
            assertThat(event.buyOrderId()).isEqualTo(buyOrderId);
            assertThat(event.buyAccountId()).isEqualTo(buyAccountId);
            assertThat(event.sellOrderId()).isEqualTo(sellOrderId);
            assertThat(event.sellAccountId()).isEqualTo(sellAccountId);
            assertThat(event.filledQuantity()).isEqualTo(4);
            assertThat(event.matchPrice()).isEqualByComparingTo("10000");
        } finally {
            publication.close();
        }
    }

    /** 주문을 인코딩해 Aeron으로 발행한다. 백프레셔면 받아들여질 때까지 재시도한다. */
    private void send(Publication publication, JournaledOrder order) {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        int length = codec.encode(buffer, 0, order);

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

    /** 이 실행의 accountId 키를 가진 레코드 두 개(매수·매도)가 도착할 때까지 폴링한다. */
    private List<ConsumerRecord<String, TradeFilledEvent>> consumeUntilBothSidesArrive(long buyAccountId, long sellAccountId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "matching-order-intake-integration-test-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        String buyKey = String.valueOf(buyAccountId);
        String sellKey = String.valueOf(sellAccountId);
        List<ConsumerRecord<String, TradeFilledEvent>> found = new ArrayList<>();
        try (KafkaConsumer<String, TradeFilledEvent> consumer =
                 new KafkaConsumer<>(props, new StringDeserializer(), new JsonDeserializer<>(TradeFilledEvent.class, false))) {
            consumer.subscribe(List.of(TOPIC));

            long deadline = System.currentTimeMillis() + 20_000; // 최초 컨슈머 그룹 조인 지연 감안(B4와 동일 이유)
            while (found.size() < 2 && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, TradeFilledEvent> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, TradeFilledEvent> record : records) {
                    if (buyKey.equals(record.key()) || sellKey.equals(record.key())) {
                        found.add(record);
                    }
                }
            }
        }
        found.sort((a, b) -> Long.compare(Long.parseLong(a.key()), Long.parseLong(b.key()))); // 매수 키가 먼저 오게 정렬(검증 편의)
        return found;
    }
}
