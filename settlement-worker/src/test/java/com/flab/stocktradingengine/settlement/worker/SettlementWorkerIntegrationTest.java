package com.flab.stocktradingengine.settlement.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.annotation.DirtiesContext;

import com.flab.stocktradingengine.kafka.KafkaTopics;
import com.flab.stocktradingengine.kafka.event.SettlementRequestEvent;
import com.flab.stocktradingengine.kafka.event.SettlementResultEvent;

/**
 * settlement-worker 앱을 실제로 띄우고, 로컬 docker-compose Kafka(localhost:9092)에 진짜
 * {@code settlement-requests} 메시지를 발행해 DB에 저장되는지, 같은 settlementRef 재도착이
 * 멱등하게 무시되는지 확인하는 end-to-end 테스트.
 *
 * <p>사전 조건: {@code docker compose up -d} 로 로컬 Kafka(9092)가 떠 있어야 한다.</p>
 *
 * <p>{@code @DirtiesContext} — account-worker 쪽 통합테스트와 같은 이유(group.id
 * "settlement-worker" 공유로 인한 교차 리밸런스 방지).</p>
 */
@SpringBootTest(classes = SettlementWorkerApplication.class)
@DirtiesContext
class SettlementWorkerIntegrationTest {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    @Autowired
    private PendingSettlementRepository pendingSettlementRepository;

    @Autowired
    private SettlementDispatcher settlementDispatcher;

    @Test
    void 만기_도래한_건을_스캔하면_실제_account_settlements로_되돌림을_발행하고_SETTLED로_마킹한다() throws Exception {
        long settlementRef = System.nanoTime();
        SettlementRequestEvent event = new SettlementRequestEvent(settlementRef, 7L, new BigDecimal("60000"), 100L);
        publish(event);
        awaitSaved(settlementRef);

        int dispatched = settlementDispatcher.dispatchDue(System.currentTimeMillis()); // dueAtEpochMillis=100 이미 지남

        assertThat(dispatched).isGreaterThanOrEqualTo(1);
        assertThat(pendingSettlementRepository.findById(settlementRef).orElseThrow().getStatus())
            .isEqualTo(SettlementStatus.SETTLED);
        assertThat(awaitAccountSettlementsMessage(settlementRef)).isTrue();
    }

    @Test
    void 실제_카프카로_받은_정산요청을_저장하고_재도착은_멱등하게_무시한다() throws Exception {
        long settlementRef = System.nanoTime(); // 실행마다 새 값 — 이전 실행 재도착과 안 헷갈리게
        SettlementRequestEvent event = new SettlementRequestEvent(settlementRef, 1L, new BigDecimal("60000"), 123L);

        publish(event);
        PendingSettlement saved = awaitSaved(settlementRef);
        assertThat(saved.getAccountId()).isEqualTo(1L);
        assertThat(saved.getAmount()).isEqualByComparingTo(new BigDecimal("60000"));
        assertThat(saved.getDueAtEpochMillis()).isEqualTo(123L);
        assertThat(saved.getStatus()).isEqualTo(SettlementStatus.PENDING);

        // settlement-requests는 다른 테스트(AccountFillIntegrationTest)도 실제로 발행하는 공유
        // 토픽이라, 이 컨슈머 그룹이 처음 뜨면 그 과거 메시지까지 같이 소비한다 — 그래서 절대
        // 행 개수(count()==1)가 아니라 이 테스트가 만든 변화(delta)로 재도착 무시를 확인한다.
        long countBeforeRetry = pendingSettlementRepository.count();
        publish(event); // 같은 settlementRef 재전송
        Thread.sleep(1000); // 재전송이 처리될 시간을 준다 — 행 개수가 그대로인지 확인하는 것이 목적
        assertThat(pendingSettlementRepository.count()).isEqualTo(countBeforeRetry);
    }

    private void publish(SettlementRequestEvent event) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        try (KafkaProducer<String, SettlementRequestEvent> producer =
                 new KafkaProducer<>(props, new StringSerializer(), new JsonSerializer<>())) {
            producer.send(new ProducerRecord<>(
                    KafkaTopics.settlementRequests(), String.valueOf(event.accountId()), event))
                .get(5, TimeUnit.SECONDS);
        }
    }

    /** account-settlements에서 이 settlementRef를 가진 SettlementResultEvent가 실제로 도착하는지 확인한다. */
    private boolean awaitAccountSettlementsMessage(long settlementRef) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "settlement-worker-test-" + settlementRef); // 매번 새 그룹 — earliest부터 다 본다
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // useHeaders=false — 타입 헤더(__TypeId__)의 trusted-packages 검사를 안 타고 targetType으로 바로 역직렬화한다.
        JsonDeserializer<SettlementResultEvent> valueDeserializer =
            new JsonDeserializer<>(SettlementResultEvent.class, false);
        try (KafkaConsumer<String, SettlementResultEvent> consumer =
                 new KafkaConsumer<>(props, new StringDeserializer(), valueDeserializer)) {
            consumer.subscribe(Collections.singletonList(KafkaTopics.accountSettlements()));
            long deadline = System.currentTimeMillis() + 20_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, SettlementResultEvent> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, SettlementResultEvent> record : records) {
                    if (record.value().settlementRef().equals(settlementRef)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private PendingSettlement awaitSaved(long settlementRef) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 20_000; // 최초 컨슈머 그룹 조인 비용 감안
        while (System.currentTimeMillis() < deadline) {
            Optional<PendingSettlement> found = pendingSettlementRepository.findById(settlementRef);
            if (found.isPresent()) {
                return found.get();
            }
            Thread.sleep(100);
        }
        throw new AssertionError("정산 요청이 20초 안에 저장되지 않았다: settlementRef=" + settlementRef);
    }
}
