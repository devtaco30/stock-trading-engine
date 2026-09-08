package com.flab.stocktradingengine.account.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.annotation.DirtiesContext;

import com.flab.stocktradingengine.account.disruptor.AccountEngine;
import com.flab.stocktradingengine.account.disruptor.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.RejectReason;
import com.flab.stocktradingengine.kafka.KafkaTopics;
import com.flab.stocktradingengine.kafka.event.SettlementResultEvent;

/**
 * account-worker 앱을 실제로 띄우고, 로컬 docker-compose Kafka(localhost:9092)에 진짜
 * {@code account-settlements} 메시지를 발행해 잔고·미수금이 실제로 깎이는지, 같은 settlementRef
 * 재도착이 멱등하게 무시되는지 확인하는 end-to-end 테스트.
 *
 * <p>사전 조건: {@code docker compose up -d} 로 로컬 Kafka(9092)가 떠 있어야 한다.</p>
 *
 * <p>{@code @DirtiesContext} — AccountFillIntegrationTest와 같은 이유(group.id "account-worker"
 * 공유로 인한 교차 리밸런스 방지).</p>
 */
@SpringBootTest(
    classes = AccountWorkerApplication.class,
    properties = {
        "account-worker.seed-accounts[0].account-id=1",
        "account-worker.seed-accounts[0].balance=1000000",
        "account-worker.seed-accounts[0].margin-rate=0.40"
    }
)
@DirtiesContext
@Import(AccountSettlementIntegrationTest.RecorderConfig.class)
class AccountSettlementIntegrationTest {

    private static final String STOCK = "005930";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final long ACCOUNT_ID = 1L;

    @Autowired
    private AccountEngine engine;

    @Autowired
    private Recorder recorder;

    @Test
    void 실제_카프카로_받은_정산을_반영하고_재도착은_멱등하게_무시한다() throws Exception {
        recorder.prepare(2); // 매수 접수(1) + 매수 체결(1, 미수금 60000 생김)
        engine.publishBuy(1001L, ACCOUNT_ID, STOCK, new BigDecimal("10000"), 10, "r1");
        engine.publishBuyFill(9001L, 1001L, ACCOUNT_ID, STOCK, new BigDecimal("10000"), 10);
        assertThat(recorder.await()).as("예약·체결 콜백이 도착해야 한다").isTrue();

        long settlementRef = System.nanoTime(); // 실행마다 새 값 — 이전 실행 재도착과 안 헷갈리게

        recorder.prepare(1);
        publishSettlement(settlementRef, ACCOUNT_ID, new BigDecimal("60000"));
        assertThat(recorder.await()).as("정산 반영 콜백이 도착해야 한다").isTrue();
        assertThat(recorder.applied()).containsExactly(true);

        recorder.prepare(1);
        publishSettlement(settlementRef, ACCOUNT_ID, new BigDecimal("60000")); // 같은 settlementRef 재전송
        assertThat(recorder.await()).as("재도착 콜백도 도착해야 한다").isTrue();
        assertThat(recorder.applied()).containsExactly(true, false); // 두 번째는 멱등 스킵
    }

    private void publishSettlement(long settlementRef, long accountId, BigDecimal amount) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        try (KafkaProducer<String, SettlementResultEvent> producer =
                 new KafkaProducer<>(props, new StringSerializer(), new JsonSerializer<>())) {
            producer.send(new ProducerRecord<>(KafkaTopics.accountSettlements(), String.valueOf(accountId),
                    new SettlementResultEvent(settlementRef, accountId, amount)))
                .get(5, TimeUnit.SECONDS);
        }
    }

    static class RecorderConfig {
        // @Primary가 아니다 — CompositeAccountResultListener(AccountEngineConfig)가
        // 이 Recorder도 delegate로 포함해 fan-out 하므로, Composite 쪽이 유일한 @Primary여야 한다.
        @Bean
        Recorder recorder() {
            return new Recorder();
        }
    }

    /** 테스트가 결과를 기다리고 검증할 수 있게 콜백을 모으는 {@link AccountResultListener}. */
    static class Recorder implements AccountResultListener {
        private final List<Boolean> applied = new CopyOnWriteArrayList<>();
        private volatile CountDownLatch latch;

        void prepare(int expectedResults) {
            latch = new CountDownLatch(expectedResults);
        }

        boolean await() throws InterruptedException {
            return latch.await(20, TimeUnit.SECONDS); // 최초 컨슈머 그룹 조인 지연 감안(B4와 동일 이유)
        }

        List<Boolean> applied() {
            return applied;
        }

        @Override
        public void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin) {
            latch.countDown();
        }

        @Override
        public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
            latch.countDown();
        }

        @Override
        public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
            latch.countDown();
        }

        @Override
        public void onFillApplied(long accountId, long orderId, long tradeId, boolean fillApplied) {
            latch.countDown();
        }

        @Override
        public void onSettlementApplied(long accountId, long settlementRef, boolean settlementApplied) {
            applied.add(settlementApplied);
            latch.countDown();
        }

        @Override
        public void onUnpaidRecorded(long accountId, long tradeId, BigDecimal amount) {
        }
    }
}
