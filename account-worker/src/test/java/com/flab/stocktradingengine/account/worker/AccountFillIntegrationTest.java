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
 *
 * <p>{@code @DirtiesContext} — 이 컨텍스트를 Spring 테스트 캐시에 남겨두지 않는다. 캐시에 남으면
 * account-fills·account-settlements 두 컨슈머가 다른 테스트 클래스가 도는 동안에도 group.id
 * "account-worker"의 멤버로 계속 붙어 있어, 그 다른 테스트가 컨슈머를 새로 join/leave 할 때마다
 * 이 컨슈머까지 같이 리밸런스에 휘말린다 — 실제로 겪은 문제(제너레이션이 계속 올라가며
 * 폴링이 지연돼 타임아웃).</p>
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
@DirtiesContext
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
        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 4, "r1");
        engine.publishSell(2L, STOCK, 4, "r2");
        assertThat(recorder.await()).as("예약 콜백이 1초 안에 도착해야 한다").isTrue();
        assertThat(recorder.rejections()).as("매수·매도 예약이 전부 통과해야 한다").isEmpty();
        // 계좌 워커가 발급한 실제 orderId(C5-2a) — 클라이언트가 정하지 않으므로 accept 콜백에서 꺼내 쓴다.
        long buyOrderId = recorder.acceptedOrderIds().get(0);
        long sellOrderId = recorder.acceptedOrderIds().get(1);

        // fan-out 메시지 2개(같은 체결, accountId 키만 다름)를 이 컨슈머가 매번 매수·매도 양쪽 다
        // AccountEngine에 넣는다(AccountFillConsumer 참고) — 이 테스트는 계좌 1·2를 워커 하나가
        // 같이 소유하므로 진짜 반영 2개 + 같은 tradeId 재도착으로 무시된 중복 2개, 총 4개가
        // 결정론적으로 온다. 2개만 기다렸다가 단언하면 "그 2개가 진짜인지 중복인지"가 경쟁이 된다.
        recorder.prepare(4);
        TradeFilledEvent fill = new TradeFilledEvent(9001L, STOCK, buyOrderId, 1L, sellOrderId, 2L, 4, new BigDecimal("10000"));
        publishFanOut(fill);

        assertThat(recorder.await()).as("체결 반영 콜백 4개(반영 2 + 중복무시 2)가 도착해야 한다").isTrue();
        List<Recorder.FillEvent> appliedEvents = recorder.fillEvents().stream()
            .filter(Recorder.FillEvent::applied)
            .toList();
        assertThat(appliedEvents).containsExactlyInAnyOrder(
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
        // @Primary가 아니다 — CompositeAccountResultListener(AccountEngineConfig)가
        // 이 Recorder도 delegate로 포함해 fan-out 하므로, Composite 쪽이 유일한 @Primary여야 한다.
        @Bean
        Recorder recorder() {
            return new Recorder();
        }
    }

    /** 테스트가 결과를 기다리고 검증할 수 있게 콜백을 모으는 {@link AccountResultListener}. */
    static class Recorder implements AccountResultListener {
        private final List<FillEvent> events = new CopyOnWriteArrayList<>();
        private final List<RejectReason> rejections = new CopyOnWriteArrayList<>();
        private final List<Long> acceptedOrderIds = new CopyOnWriteArrayList<>();
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

        /** 계좌 워커가 매수·매도 접수 순서대로 발급한 orderId(C5-2a) — 클라이언트가 정하지 않아 fill 이벤트를 만들 때 여기서 꺼내 쓴다. */
        List<Long> acceptedOrderIds() {
            return acceptedOrderIds;
        }

        @Override
        public void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin) {
            acceptedOrderIds.add(orderId);
            latch.countDown();
        }

        @Override
        public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
            acceptedOrderIds.add(orderId);
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

        @Override
        public void onSettlementApplied(long accountId, long settlementRef, boolean applied) {
            latch.countDown();
        }

        @Override
        public void onUnpaidRecorded(long accountId, long tradeId, BigDecimal amount) {
        }

        @Override
        public void onDuplicateRequest(long accountId, long orderId, String requestId) {
        }

        record FillEvent(long accountId, long tradeId, boolean applied) {
        }
    }
}
