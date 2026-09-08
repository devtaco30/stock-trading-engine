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
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.flab.stocktradingengine.account.disruptor.AccountEngine;
import com.flab.stocktradingengine.account.disruptor.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.RejectReason;
import com.flab.stocktradingengine.kafka.KafkaTopics;
import com.flab.stocktradingengine.kafka.event.TradeFilledEvent;

/**
 * account-worker 앱을 실제로 띄우고, 로컬 docker-compose Kafka(localhost:9092)에 진짜
 * {@code account-fills} 메시지를 발행해 계좌에 반영되는지 확인하는 end-to-end 테스트.
 *
 * <p>주문 접수 경로(Aeron)는 B4 범위 밖이라, 체결이 도착하기 전에 필요한 매수·매도 예약은
 * 테스트가 {@link AccountEngine}에 직접 발행해 만든다 — "이미 예약된 주문에 체결이
 * 도착한다"는 전제를 흉내낸다.</p>
 *
 * <p>사전 조건: {@code docker compose up -d} 로 로컬 Kafka(9092)가 떠 있어야 한다.</p>
 */
@SpringBootTest(
    classes = AccountWorkerApplication.class,
    properties = {
        "account-worker.seed-accounts[0].account-id=1",
        "account-worker.seed-accounts[0].balance=1000000",
        "account-worker.seed-accounts[0].margin-rate=0.40",
        "account-worker.seed-accounts[1].account-id=2",
        "account-worker.seed-accounts[1].balance=1000000",
        "account-worker.seed-accounts[1].margin-rate=0.40",
        "account-worker.seed-accounts[1].holdings[005930]=10"
    }
)
@Import(AccountFillIntegrationTest.RecorderConfig.class)
class AccountFillIntegrationTest {

    private static final String STOCK = "005930";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    @Autowired
    private AccountEngine engine;

    @Autowired
    private Recorder recorder;

    @Test
    void 실제_카프카로_받은_체결을_매수_매도_양쪽에_반영한다() throws Exception {
        recorder.prepare(2); // 매수 예약(1) + 매도 예약(1)
        engine.publishBuy(1001L, 1L, STOCK, new BigDecimal("10000"), 4, "r1");
        engine.publishSell(2001L, 2L, STOCK, 4, "r2");
        assertThat(recorder.await()).as("예약 콜백이 1초 안에 도착해야 한다").isTrue();
        assertThat(recorder.rejections()).as("매수·매도 예약이 전부 통과해야 한다").isEmpty();

        recorder.prepare(2); // 체결 반영(매수)(1) + 체결 반영(매도)(1)
        TradeFilledEvent fill = new TradeFilledEvent(9001L, STOCK, 1001L, 1L, 2001L, 2L, 4, new BigDecimal("10000"));
        publishFanOut(fill);

        assertThat(recorder.await()).as("체결 반영 콜백이 5초 안에 도착해야 한다").isTrue();
        assertThat(recorder.fillEvents()).containsExactlyInAnyOrder(
            new Recorder.FillEvent(1L, 9001L, true),
            new Recorder.FillEvent(2L, 9001L, true)
        );
    }

    /** 체결 하나를 매수·매도 계좌 앞으로 각각 accountId 키로 발행한다(ADR-018 fan-out). */
    private void publishFanOut(TradeFilledEvent fill) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        try (KafkaProducer<String, TradeFilledEvent> producer =
                 new KafkaProducer<>(props, new StringSerializer(), new JsonSerializer<>())) {
            producer.send(new ProducerRecord<>(KafkaTopics.accountFills(), String.valueOf(fill.buyAccountId()), fill))
                .get(5, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(KafkaTopics.accountFills(), String.valueOf(fill.sellAccountId()), fill))
                .get(5, TimeUnit.SECONDS);
        }
    }

    static class RecorderConfig {
        @Bean
        @Primary
        Recorder recorder() {
            return new Recorder();
        }
    }

    /** 테스트가 결과를 기다리고 검증할 수 있게 콜백을 모으는 {@link AccountResultListener}. */
    static class Recorder implements AccountResultListener {
        private final List<FillEvent> events = new CopyOnWriteArrayList<>();
        private final List<RejectReason> rejections = new CopyOnWriteArrayList<>();
        private volatile CountDownLatch latch;

        void prepare(int expectedResults) {
            latch = new CountDownLatch(expectedResults);
        }

        boolean await() throws InterruptedException {
            // 갓 띄운 로컬 브로커는 컨슈머 그룹 코디네이터 협상(__consumer_offsets 생성 등)에
            // 수 초가 걸릴 수 있어 넉넉히 잡는다(실제 처리 지연이 아니라 최초 조인 비용).
            return latch.await(20, TimeUnit.SECONDS);
        }

        List<FillEvent> fillEvents() {
            return events;
        }

        List<RejectReason> rejections() {
            return rejections;
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
            rejections.add(reason);
            latch.countDown();
        }

        @Override
        public void onFillApplied(long accountId, long orderId, long tradeId, boolean applied) {
            events.add(new FillEvent(accountId, tradeId, applied));
            latch.countDown();
        }

        record FillEvent(long accountId, long tradeId, boolean applied) {
        }
    }
}
