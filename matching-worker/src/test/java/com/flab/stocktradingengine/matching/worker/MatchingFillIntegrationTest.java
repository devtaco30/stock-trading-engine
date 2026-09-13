package com.flab.stocktradingengine.matching.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * matching-worker 앱을 실제로 띄우고, 교차 주문을 넣어 체결이 나면 로컬 docker-compose
 * Kafka(localhost:9092)의 {@code account-fills} 토픽에 매수·매도 두 메시지가 같은 tradeId로
 * 실제 발행되는지 확인하는 end-to-end 테스트.
 *
 * <p>사전 조건: {@code docker compose up -d} 로 로컬 Kafka(9092)가 떠 있어야 한다.</p>
 *
 * <p>{@code @DirtiesContext} — 이 테스트가 직접 {@code engine.publishPlace}를 테스트 스레드에서
 * 호출한다. Spring 테스트 컨텍스트 캐싱으로 이 컨텍스트가 살아남으면, 같은 설정으로 뜨는 다른
 * {@code @SpringBootTest}(예: {@code MatchingOrderIntakeIntegrationTest})가 캐시를 재사용해 같은
 * {@code MatchingEngine} 빈을 공유하게 되고, 그 빈의 링버퍼(ProducerType.SINGLE)가 "첫 발행 스레드"로
 * 이 테스트 스레드를 이미 기억한 상태라 Aeron 수신 스레드가 처음 발행할 때
 * {@code AssertionError: Accessed by two threads}로 깨진다(실제로 겪은 문제) — 컨텍스트를 남기지 않는다.</p>
 */
@SpringBootTest(classes = MatchingWorkerApplication.class)
@DirtiesContext
class MatchingFillIntegrationTest {

    private static final String STOCK = "005930";
    private static final String TOPIC = KafkaTopics.accountFills();
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    @Autowired
    private MatchingEngine engine;

    @Test
    void 교차주문이_체결되면_account_fills에_매수_매도_같은_tradeId로_두건_발행된다() throws Exception {
        // 계좌·주문 ID를 실행마다 새로 뽑는다 — 상수로 고정하면 이전 실행이 이 토픽에 남긴
        // 레코드까지 조건에 걸려서, 이번 실행이 실제로 발행 안 해도 옛 레코드로 테스트가
        // 통과해버린다(리뷰 발견: 검증이 아니라 우연히 가려짐).
        long runId = System.nanoTime();
        long buyAccountId = runId;
        long sellAccountId = runId + 1;
        long buyOrderId = runId + 2;
        long sellOrderId = runId + 3;

        Instant now = Instant.now();
        engine.publishPlace(buyOrderId, buyAccountId, STOCK, OrderSide.BUY, new BigDecimal("10000"), 4, now);
        engine.publishPlace(sellOrderId, sellAccountId, STOCK, OrderSide.SELL, new BigDecimal("10000"), 4, now.plusMillis(1));

        List<ConsumerRecord<String, TradeFilledEvent>> matched = consumeUntilBothSidesArrive(buyAccountId, sellAccountId);

        assertThat(matched).hasSize(2);
        ConsumerRecord<String, TradeFilledEvent> buySide = matched.get(0);
        ConsumerRecord<String, TradeFilledEvent> sellSide = matched.get(1);

        assertThat(buySide.key()).isEqualTo(String.valueOf(buyAccountId));
        assertThat(sellSide.key()).isEqualTo(String.valueOf(sellAccountId));
        assertThat(buySide.value().tradeId()).isEqualTo(sellSide.value().tradeId());
        assertThat(buySide.value()).isEqualTo(sellSide.value()); // 같은 TradeFilledEvent(양쪽 다 담김)

        TradeFilledEvent event = buySide.value();
        assertThat(event.stockCode()).isEqualTo(STOCK);
        assertThat(event.buyOrderId()).isEqualTo(buyOrderId);
        assertThat(event.buyAccountId()).isEqualTo(buyAccountId);
        assertThat(event.sellOrderId()).isEqualTo(sellOrderId);
        assertThat(event.sellAccountId()).isEqualTo(sellAccountId);
        assertThat(event.filledQuantity()).isEqualTo(4);
        assertThat(event.matchPrice()).isEqualByComparingTo("10000");
    }

    /** 이 실행의 accountId 키를 가진 레코드 두 개(매수·매도)가 도착할 때까지 폴링한다. */
    private List<ConsumerRecord<String, TradeFilledEvent>> consumeUntilBothSidesArrive(long buyAccountId, long sellAccountId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "matching-fill-integration-test-" + System.nanoTime());
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
