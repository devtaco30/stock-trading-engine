package com.flab.stocktradingengine.account.projection.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import com.flab.stocktradingengine.account.projection.worker.entity.AccountProjection;
import com.flab.stocktradingengine.account.projection.worker.entity.AccountProjectionHolding;
import com.flab.stocktradingengine.account.projection.worker.repository.AccountProjectionHoldingRepository;
import com.flab.stocktradingengine.account.projection.worker.repository.AccountProjectionRepository;
import com.flab.stocktradingengine.kafka.KafkaTopics;
import com.flab.stocktradingengine.kafka.event.AccountStateEvent;

/**
 * account-projection-worker 앱을 실제로 띄우고, 임베디드 Kafka로 진짜 {@code account-state}
 * 이벤트를 발행해 JSON 역직렬화 → stale-guard upsert가 실제로 도는지 확인하는 end-to-end
 * 테스트(계좌 상태 영속/프로젝션 트랙 U3). account-worker(U2)가 발행하는 형식과 이 워커가
 * 소비하는 형식이 실제로 맞는지(JSON 직렬화 경로)가 여기서 처음 검증된다.
 *
 * <p>DB는 기본 프로파일(H2, 인메모리)이다 — read model이라 재구성 가능해 내구성 경계가
 * 아니다(settlement-worker가 실 Postgres로 검증하는 것과 다른 이유).</p>
 */
@SpringBootTest(
    classes = AccountProjectionWorkerApplication.class,
    properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 1, topics = "account-state")
@DirtiesContext
class AccountProjectionWorkerIntegrationTest {

    private static final String STOCK = "005930";

    @Value("${spring.embedded.kafka.brokers}")
    private String bootstrapServers;

    @Autowired
    private AccountProjectionRepository accountProjectionRepository;

    @Autowired
    private AccountProjectionHoldingRepository accountProjectionHoldingRepository;

    @Test
    void 발행된_이벤트를_소비하면_read_model에_잔고_보유_seq가_반영된다() throws Exception {
        long accountId = System.nanoTime();
        publish(new AccountStateEvent(accountId, new BigDecimal("900000"), Map.of(STOCK, 10), 1L, 123L));

        AccountProjection projection = awaitProjection(accountId, 1L);

        assertThat(projection.getBalance()).isEqualByComparingTo("900000");
        assertThat(projection.getSeq()).isEqualTo(1L);
        List<AccountProjectionHolding> holdings = accountProjectionHoldingRepository.findByAccountId(accountId);
        assertThat(holdings).hasSize(1);
        assertThat(holdings.get(0).getQuantity()).isEqualTo(10);
    }

    @Test
    void stale_seq는_반영되지_않는다() throws Exception {
        long accountId = System.nanoTime();
        publish(new AccountStateEvent(accountId, new BigDecimal("900000"), Map.of(STOCK, 10), 3L, 100L));
        awaitProjection(accountId, 3L);

        publish(new AccountStateEvent(accountId, new BigDecimal("999999"), Map.of(STOCK, 999), 2L, 200L)); // stale(2 <= 3)
        Thread.sleep(1000); // 반영 안 됨을 확인하기 위한 대기 — 처리될 시간을 준 뒤에도 그대로인지 본다

        AccountProjection projection = accountProjectionRepository.findById(accountId).orElseThrow();
        assertThat(projection.getSeq()).isEqualTo(3L);
        assertThat(projection.getBalance()).isEqualByComparingTo("900000");
    }

    @Test
    void 같은_계좌_이벤트_여러개가_한_배치로_묶여도_최신_seq만_반영된다() throws Exception {
        long accountId = System.nanoTime();
        // 컨슈머가 폴링하기 전에 연속으로 발행 — 실제 poll 배치 하나로 묶여 소비될 가능성이 높다
        // (배치로 안 묶여도 각 이벤트가 순서대로 stale-guard를 통과해 결국 같은 최종 상태가 된다 —
        // 이 테스트는 "배치여도 결과가 맞다"를 보는 것이지, 배치 크기를 직접 관측하지 않는다).
        for (long seq = 1; seq <= 5; seq++) {
            publish(new AccountStateEvent(accountId, BigDecimal.valueOf(seq * 100), Map.of(STOCK, (int) seq), seq, 100L * seq));
        }

        AccountProjection projection = awaitProjection(accountId, 5L);

        assertThat(projection.getSeq()).isEqualTo(5L);
        assertThat(projection.getBalance()).isEqualByComparingTo("500");
        List<AccountProjectionHolding> holdings = accountProjectionHoldingRepository.findByAccountId(accountId);
        assertThat(holdings).hasSize(1);
        assertThat(holdings.get(0).getQuantity()).isEqualTo(5);
    }

    @Test
    void 같은_이벤트가_재도착해도_멱등하게_무시한다() throws Exception {
        long accountId = System.nanoTime();
        AccountStateEvent event = new AccountStateEvent(accountId, new BigDecimal("900000"), Map.of(STOCK, 10), 1L, 123L);
        publish(event);
        awaitProjection(accountId, 1L);

        publish(event); // 재도착(같은 seq)
        Thread.sleep(1000);

        AccountProjection projection = accountProjectionRepository.findById(accountId).orElseThrow();
        assertThat(projection.getSeq()).isEqualTo(1L);
        assertThat(projection.getBalance()).isEqualByComparingTo("900000");
    }

    private void publish(AccountStateEvent event) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (KafkaProducer<String, AccountStateEvent> producer =
                 new KafkaProducer<>(props, new StringSerializer(), new JsonSerializer<>())) {
            producer.send(new ProducerRecord<>(
                    KafkaTopics.accountState(), String.valueOf(event.accountId()), event))
                .get(5, TimeUnit.SECONDS);
        }
    }

    private AccountProjection awaitProjection(long accountId, long expectedSeq) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 20_000; // 최초 컨슈머 그룹 조인 비용 감안
        while (System.currentTimeMillis() < deadline) {
            Optional<AccountProjection> found = accountProjectionRepository.findById(accountId);
            if (found.isPresent() && found.get().getSeq() >= expectedSeq) {
                return found.get();
            }
            Thread.sleep(100);
        }
        throw new AssertionError("프로젝션이 20초 안에 반영되지 않았다: accountId=" + accountId);
    }
}
