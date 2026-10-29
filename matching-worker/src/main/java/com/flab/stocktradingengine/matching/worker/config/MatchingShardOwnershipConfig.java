package com.flab.stocktradingengine.matching.worker.config;

import java.util.List;
import java.util.Properties;
import java.util.function.IntPredicate;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.aeron.StockSlotHasher;
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventListener;
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventRequests;
import com.flab.stocktradingengine.matching.worker.coordination.KafkaMatchingShardAssignment;
import com.flab.stocktradingengine.matching.worker.coordination.MapSync;
import com.flab.stocktradingengine.matching.worker.coordination.MatchingSlotMapReader;
import com.flab.stocktradingengine.matching.worker.coordination.MovedSlotRequests;
import com.flab.stocktradingengine.matching.worker.handover.HandoverCoordinator;
import com.flab.stocktradingengine.matching.worker.handover.HandoverBookStore;
import com.flab.stocktradingengine.matching.worker.lifecycle.KafkaMatchingShardAssignmentLifecycle;

import lombok.extern.slf4j.Slf4j;

/**
 * 이 매칭 shard가 담당하는 종목 slot 판정({@link IntPredicate})을 두 소스 중 정확히 하나로 고정한다.
 * {@code matching-shard.coordination.enabled}가 켜져 있으면 Kafka 컨슈머 그룹 배정
 * ({@link KafkaMatchingShardAssignment}), 꺼져 있으면 모든 종목 slot을 담당한다(기본값, 샤딩을 모르는
 * 기존 실행 구성과 테스트가 그대로 모든 종목을 받는다). 둘이 함께 등록되면 어느 쪽이 담당을 정했는지
 * 추적할 수 없으므로 {@link ConditionalOnProperty}로 배타적으로 등록한다.
 *
 * <p>담당 확인은 {@code ownedSlots.test(stockSlotHasher.slotFor(stockCode))}다. 종목 slot 개수는
 * 계좌 쪽 slot 개수와 별개로 {@code matching-shard.stock-slot-count}(기본 64)이다.</p>
 */
@Slf4j
@Configuration
public class MatchingShardOwnershipConfig {

    static final int DEFAULT_STOCK_SLOT_COUNT = 64;

    @Bean
    public StockSlotHasher stockSlotHasher(
            @Value("${matching-shard.stock-slot-count:" + DEFAULT_STOCK_SLOT_COUNT + "}") int stockSlotCount) {
        log.info("[매칭] 종목 slot 개수: {}", stockSlotCount);
        return new StockSlotHasher(stockSlotCount);
    }

    /** Kafka 배정 콜백 thread가 종목 slot 받음·넘김 요청을 넣고 주문 수신 thread가 꺼내 이벤트로 발행하는 통로. */
    @Bean
    public SlotEventRequests slotEventRequests() {
        return new SlotEventRequests();
    }

    /**
     * 받음 이벤트가 저널에 기록되면 그 종목 slot의 담당을 켠다. 조정 모드가 꺼져 있으면 배정 빈이 없어 아무 일도 하지 않는다.
     * 엔진 빈이 이 빈을 받고 배정 빈은 엔진을 모르므로 순환이 없다.
     */
    @Bean
    public SlotEventListener matchingSlotEventListener(
            ObjectProvider<KafkaMatchingShardAssignment> assignmentProvider, ObjectProvider<HandoverBookStore> handoverBookStore) {
        return new SlotEventListener() {
            @Override
            public void onSlotAcquiredJournaled(int stockSlot) {
                KafkaMatchingShardAssignment assignment = assignmentProvider.getIfAvailable();
                if (assignment != null) {
                    assignment.confirmAcquired(stockSlot);
                }
                // 넘겼던 종목 slot을 다시 받았으면 보관한 호가창 바이트는 더 이상 보낼 이유가 없다(11-13절 4번).
                HandoverBookStore store = handoverBookStore.getIfAvailable();
                if (store != null) {
                    store.remove(stockSlot);
                }
            }

            @Override
            public void onBooksLoadedDurable(long transferId, int stockSlot) {
                // 이동으로 받은 종목 slot의 호가창 적재와 스냅샷 확정이 끝났다. 이제 받음 요청을 남기고 저널 기록 확인이 오면 담당이 켜진다.
                // 이전 담당(X)에게 받았다고 알리는 일은 U16이 한다.
                log.info("[호가창 이동] 적재 확정: 이동 번호={} 종목 slot={}", transferId, stockSlot);
                KafkaMatchingShardAssignment assignment = assignmentProvider.getIfAvailable();
                if (assignment != null) {
                    assignment.acquireAfterLoad(stockSlot);
                }
            }

            @Override
            public void onSlotCleanedUp(int stockSlot) {
                // 새 담당이 확정되어 호가창을 지웠다. 보관한 바이트도 더 보낼 이유가 없다(11-14절 4번).
                HandoverBookStore store = handoverBookStore.getIfAvailable();
                if (store != null) {
                    store.remove(stockSlot);
                }
            }

            @Override
            public void onSlotHandedOver(int stockSlot, List<byte[]> bookBytesPerStock) {
                // 넘김 이벤트를 처리한 엔진 thread가 부른다. 요청이 오면 호가창 발신 thread가 이 바이트를 보낸다.
                HandoverBookStore store = handoverBookStore.getIfAvailable();
                if (store != null) {
                    store.put(stockSlot, bookBytesPerStock);
                }
            }
        };
    }

    @Bean
    @ConditionalOnProperty(prefix = "matching-shard.coordination", name = "enabled", havingValue = "false", matchIfMissing = true)
    public IntPredicate allStockSlotsOwned() {
        log.info("[매칭] 담당 종목 slot 소스: 모든 종목 slot을 담당한다(matching-shard.coordination.enabled=false)");
        return slot -> true;
    }

    /**
     * 이 빈이 담당 판정({@link IntPredicate})을 겸한다. 판정용 빈을 따로 한 겹 더 두면 후보가 둘이 되어
     * Spring이 어느 것을 넣을지 정하지 못하고 기동이 멈춘다.
     */
    @Bean
    @ConditionalOnProperty(prefix = "matching-shard.coordination", name = "enabled", havingValue = "true")
    public KafkaMatchingShardAssignment kafkaMatchingShardAssignment(
            StockSlotHasher stockSlotHasher,
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${matching-worker.instance-id:}") String instanceIdConfig,
            @Value("${transport.matching-intake.channel:" + MatchingOrderIntakeConfig.DEFAULT_INTAKE_CHANNEL + "}") String orderEndpoint,
            @Value("${matching-shard.coordination.session-timeout-ms:120000}") int sessionTimeoutMs,
            SlotEventRequests slotEventRequests,
            ObjectProvider<MatchingSlotMapReader> mapReader, ObjectProvider<HandoverCoordinator> handoverCoordinator,
            ObjectProvider<MapSync> mapSync,
            @Value("${matching-shard.rebalance.map-write-deadline-ms:30000}") long mapWriteDeadlineMillis,
            // 같은 맵 쓰기가 브로커 오류로 연속 이만큼 실패하면 ERROR를 남기고 queue 뒤로 보낸다. 기본값 60번은 근거 없는 임의 값이다(미측정).
            @Value("${matching-shard.rebalance.map-write-failure-limit:60}") int mapWriteFailureLimit) {
        String instanceId = resolveInstanceId(instanceIdConfig, orderEndpoint);
        log.info("[매칭] 담당 종목 slot 소스: Kafka 컨슈머 그룹 배정({}) — 신원={} 주문 입구={} session.timeout.ms={}",
            KafkaMatchingShardAssignment.GROUP_ID, instanceId, orderEndpoint, sessionTimeoutMs);
        Properties consumerProps = KafkaMatchingShardAssignment.consumerProperties(
            bootstrapServers, KafkaMatchingShardAssignment.GROUP_ID, instanceId, sessionTimeoutMs);
        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        Producer<Integer, String> mapProducer = buildMapProducer(bootstrapServers);
        Admin adminClient = Admin.create(adminProps(bootstrapServers));
        java.util.concurrent.atomic.AtomicReference<KafkaMatchingShardAssignment> assignmentRef = new java.util.concurrent.atomic.AtomicReference<>();
        KafkaMatchingShardAssignment assignment = new KafkaMatchingShardAssignment(
            consumer, mapProducer, adminClient, stockSlotHasher.stockSlotCount(), orderEndpoint,
            KafkaMatchingShardAssignment.ASSIGNMENT_TOPIC, KafkaMatchingShardAssignment.MAP_TOPIC, slotEventRequests,
            slot -> mapReader.getObject().targetFor(slot), new MovedSlotRequests() {
                @Override
                public void requestHandover(int stockSlot) {
                    HandoverCoordinator coordinator = handoverCoordinator.getObject();
                    // 재요청 직전에 그 종목 slot이 아직 호가창 적재를 기다리는지 확인하게 한다. 같은 값을 다시 정해도 같다.
                    coordinator.setStillAwaitingLoad(slot -> assignmentRef.get().isAwaitingLoad(slot));
                    coordinator.requestHandover(stockSlot);
                }

                @Override
                public void cancelHandover(int stockSlot) {
                    handoverCoordinator.getObject().cancelHandover(stockSlot);
                }
            }, mapSync.getIfAvailable(), java.util.concurrent.Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(runnable, "matching-shard-assignment-decision");
                thread.setDaemon(true);
                return thread;
            }));
        assignmentRef.set(assignment);
        assignment.setMapWriteDeadlineMillis(mapWriteDeadlineMillis);
        assignment.setMapWriteFailureLimit(mapWriteFailureLimit);
        return assignment;
    }

    @Bean
    @ConditionalOnProperty(prefix = "matching-shard.coordination", name = "enabled", havingValue = "true")
    public SmartLifecycle kafkaMatchingShardAssignmentLifecycle(KafkaMatchingShardAssignment kafkaMatchingShardAssignment) {
        return new KafkaMatchingShardAssignmentLifecycle(kafkaMatchingShardAssignment);
    }

    /**
     * {@code matching-worker.instance-id}가 이 매칭 shard의 정적 멤버십 신원(group.instance.id)이다.
     * 비어 있으면 자기 주문 입구 주소로 폴백한다. 같은 신원이 세션 시간 안에 돌아오면 같은 종목 slot을
     * 돌려받는다.
     */
    static String resolveInstanceId(String instanceIdConfig, String fallbackOrderEndpoint) {
        if (instanceIdConfig != null && !instanceIdConfig.isBlank()) {
            return instanceIdConfig;
        }
        log.warn("[매칭] matching-worker.instance-id 미설정 — transport.matching-intake.channel로 폴백: {}", fallbackOrderEndpoint);
        return fallbackOrderEndpoint;
    }

    private Producer<Integer, String> buildMapProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    private Properties adminProps(String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return props;
    }
}
