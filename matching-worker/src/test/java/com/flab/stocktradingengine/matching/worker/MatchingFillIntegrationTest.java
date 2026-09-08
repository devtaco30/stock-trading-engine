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

import com.flab.stocktradingengine.kafka.event.TradeFilledEvent;
import com.flab.stocktradingengine.matching.disruptor.MatchingEngine;
import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * matching-worker 앱을 실제로 띄우고, 교차 주문을 넣어 체결이 나면 로컬 docker-compose
 * Kafka(localhost:9092)의 {@code account-fills} 토픽에 매수·매도 두 메시지가 같은 tradeId로
 * 실제 발행되는지 확인하는 end-to-end 테스트.
 *
 * <p>사전 조건: {@code docker compose up -d} 로 로컬 Kafka(9092)가 떠 있어야 한다.</p>
 */
@SpringBootTest(classes = MatchingWorkerApplication.class)
class MatchingFillIntegrationTest {

    private static final String STOCK = "005930";
    private static final String TOPIC = "account-fills";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final long BUY_ACCOUNT_ID = 9100L;
    private static final long SELL_ACCOUNT_ID = 9200L;

    @Autowired
    private MatchingEngine engine;

    @Test
    void 교차주문이_체결되면_account_fills에_매수_매도_같은_tradeId로_두건_발행된다() throws Exception {
        Instant now = Instant.now();
        engine.publishPlace(7001L, BUY_ACCOUNT_ID, STOCK, OrderSide.BUY, new BigDecimal("10000"), 4, now);
        engine.publishPlace(7002L, SELL_ACCOUNT_ID, STOCK, OrderSide.SELL, new BigDecimal("10000"), 4, now.plusMillis(1));

        List<ConsumerRecord<String, TradeFilledEvent>> matched = consumeUntilBothSidesArrive();

        assertThat(matched).hasSize(2);
        ConsumerRecord<String, TradeFilledEvent> buySide = matched.get(0);
        ConsumerRecord<String, TradeFilledEvent> sellSide = matched.get(1);

        assertThat(buySide.key()).isEqualTo(String.valueOf(BUY_ACCOUNT_ID));
        assertThat(sellSide.key()).isEqualTo(String.valueOf(SELL_ACCOUNT_ID));
        assertThat(buySide.value().tradeId()).isEqualTo(sellSide.value().tradeId());
        assertThat(buySide.value()).isEqualTo(sellSide.value()); // 같은 TradeFilledEvent(양쪽 다 담김)

        TradeFilledEvent event = buySide.value();
        assertThat(event.stockCode()).isEqualTo(STOCK);
        assertThat(event.buyOrderId()).isEqualTo(7001L);
        assertThat(event.buyAccountId()).isEqualTo(BUY_ACCOUNT_ID);
        assertThat(event.sellOrderId()).isEqualTo(7002L);
        assertThat(event.sellAccountId()).isEqualTo(SELL_ACCOUNT_ID);
        assertThat(event.filledQuantity()).isEqualTo(4);
        assertThat(event.matchPrice()).isEqualByComparingTo("10000");
    }

    /** 이 테스트의 accountId 키를 가진 레코드 두 개(매수·매도)가 도착할 때까지 폴링한다. */
    private List<ConsumerRecord<String, TradeFilledEvent>> consumeUntilBothSidesArrive() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "matching-fill-integration-test-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        List<ConsumerRecord<String, TradeFilledEvent>> found = new ArrayList<>();
        try (KafkaConsumer<String, TradeFilledEvent> consumer =
                 new KafkaConsumer<>(props, new StringDeserializer(), new JsonDeserializer<>(TradeFilledEvent.class, false))) {
            consumer.subscribe(List.of(TOPIC));

            long deadline = System.currentTimeMillis() + 20_000; // 최초 컨슈머 그룹 조인 지연 감안(B4와 동일 이유)
            while (found.size() < 2 && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, TradeFilledEvent> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, TradeFilledEvent> record : records) {
                    if (found.size() >= 2) {
                        break; // 이전 테스트 실행이 같은 토픽에 남긴 레코드까지 더 읽지 않는다
                    }
                    if (String.valueOf(BUY_ACCOUNT_ID).equals(record.key()) || String.valueOf(SELL_ACCOUNT_ID).equals(record.key())) {
                        found.add(record);
                    }
                }
            }
        }
        found.sort((a, b) -> Long.compare(Long.parseLong(a.key()), Long.parseLong(b.key()))); // 매수(9100) 키가 먼저 오게 정렬(검증 편의)
        return found;
    }
}
