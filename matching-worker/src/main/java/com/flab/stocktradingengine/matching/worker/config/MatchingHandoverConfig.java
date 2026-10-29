package com.flab.stocktradingengine.matching.worker.config;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import com.flab.stocktradingengine.aeron.StockSlotHasher;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventRequests;
import com.flab.stocktradingengine.matching.worker.coordination.HandoverEndpointPool;
import com.flab.stocktradingengine.matching.worker.coordination.KafkaMatchingShardAssignment;
import com.flab.stocktradingengine.matching.worker.coordination.StuckHandoverReporter;
import com.flab.stocktradingengine.matching.worker.coordination.MapCatchUpWaiter;
import com.flab.stocktradingengine.matching.worker.coordination.MapSync;
import com.flab.stocktradingengine.matching.worker.coordination.MatchingSlotMapReader;
import com.flab.stocktradingengine.matching.worker.handover.HandoverBookStore;
import com.flab.stocktradingengine.matching.worker.handover.HandoverCleanupTrigger;
import com.flab.stocktradingengine.matching.worker.handover.HandoverCoordinator;
import com.flab.stocktradingengine.matching.worker.handover.HandoverReceiver;
import com.flab.stocktradingengine.matching.worker.handover.HandoverRequester;
import com.flab.stocktradingengine.matching.worker.handover.HandoverResponder;
import com.flab.stocktradingengine.matching.worker.handover.HandoverRestartRule;
import com.flab.stocktradingengine.matching.worker.recovery.OwnedStockSlotRestorer;
import com.flab.stocktradingengine.matching.worker.recovery.StoredMatchingSnapshot;
import com.flab.stocktradingengine.matching.worker.lifecycle.HandoverLifecycle;

import io.aeron.Aeron;

/**
 * 호가창 이동(X→Z)의 구성요소 배선(U15a). 조정 모드({@code matching-shard.coordination.enabled=true})에서만 만든다.
 * 주소 풀은 설정 {@code matching-shard.handover.endpoints}(최대 8개, 값은 매칭 pod의 주문 입구 주소)다.
 * 요청·응답은 새 포트를 열지 않고 자기 주문 입구 주소({@code transport.matching-intake.channel})에서 스트림 번호
 * 2008·2009만 더 구독한다(11-11절). 배정 콜백이 이 구성요소를 부르는 연결은 U15b가 한다.
 */
@Configuration
@ConditionalOnProperty(prefix = "matching-shard.coordination", name = "enabled", havingValue = "true")
public class MatchingHandoverConfig {

    // 맵 읽기는 배정보다 먼저 시작하고 나중에 멈춘다(배정 생명주기가 -2).
    private static final int MAP_READER_PHASE = -3;
    // 맵 읽기(-3) 뒤, 엔진 시작(0) 전.
    private static final int RESTART_RULE_PHASE = -1;
    // 요청 응답·수신 thread는 엔진(0)과 주문 수신(1)이 돈 뒤에 시작한다.
    private static final int TRANSPORT_PHASE = 2;

    @Bean
    public HandoverEndpointPool handoverEndpointPool(
            Environment environment,
            @Value("${transport.matching-intake.channel:" + MatchingOrderIntakeConfig.DEFAULT_INTAKE_CHANNEL + "}") String ownChannel) {
        List<String> endpoints = Binder.get(environment)
            .bind("matching-shard.handover.endpoints", Bindable.listOf(String.class)).orElse(List.of());
        return HandoverEndpointPool.requireForCoordination(endpoints, ownChannel);
    }

    @Bean
    public HandoverBookStore handoverBookStore() {
        return new HandoverBookStore();
    }

    @Bean
    public MatchingSlotMapReader matchingSlotMapReader(
            StockSlotHasher stockSlotHasher, @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new MatchingSlotMapReader(new KafkaConsumer<>(props), stockSlotHasher.stockSlotCount(),
            KafkaMatchingShardAssignment.MAP_TOPIC);
    }

    /** 맵 전환을 보고 넘긴 종목 slot의 정리를 요청한다(U16b). 요청은 주문 수신 thread가 이벤트로 발행한다. */
    @Bean
    public HandoverCleanupTrigger handoverCleanupTrigger(
            MatchingSlotMapReader reader, HandoverBookStore store, SlotEventRequests slotEventRequests,
            @Value("${transport.matching-intake.channel:" + MatchingOrderIntakeConfig.DEFAULT_INTAKE_CHANNEL + "}") String ownChannel) {
        HandoverCleanupTrigger trigger = new HandoverCleanupTrigger(ownChannel, store, slotEventRequests::requestCleanup);
        reader.addListener(trigger::onMapChange);
        return trigger;
    }

    /**
     * 배정 콜백이 이동으로 받는지 판정하기 전에 맵 읽기가 맵 토픽의 끝까지 따라오기를 기다리는 장치. 기다리는 한도는
     * {@code matching-shard.coordination.map-decision-timeout-ms}(기본 10초, 근거 없는 임의 값)이다.
     */
    @Bean(destroyMethod = "close")
    public MapSyncResources matchingMapSync(
            MatchingSlotMapReader reader, @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${matching-shard.coordination.map-decision-timeout-ms:10000}") long timeoutMillis) {
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(
            java.util.Map.of(org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
        org.apache.kafka.common.TopicPartition partition = new org.apache.kafka.common.TopicPartition(
            KafkaMatchingShardAssignment.MAP_TOPIC, 0);
        java.util.function.LongSupplier endOffset = () -> {
            try {
                return admin.listOffsets(java.util.Map.of(partition, org.apache.kafka.clients.admin.OffsetSpec.latest()))
                    .partitionResult(partition).get().offset();
            } catch (java.util.concurrent.ExecutionException e) {
                throw new IllegalStateException("맵 토픽 끝 위치 조회 실패", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("맵 토픽 끝 위치 조회 중 인터럽트됨", e);
            }
        };
        return new MapSyncResources(new MapCatchUpWaiter(endOffset, () -> reader.consumedOffset(0), timeoutMillis), admin, reader::isHealthy);
    }

    /** 판정 대기 장치와 그것이 쓰는 Admin을 같이 닫기 위한 묶음. */
    public static final class MapSyncResources implements AutoCloseable, MapSync {
        private final MapSync delegate;
        private final org.apache.kafka.clients.admin.Admin admin;

        private final java.util.function.BooleanSupplier healthy;

        MapSyncResources(MapSync delegate, org.apache.kafka.clients.admin.Admin admin, java.util.function.BooleanSupplier healthy) {
            this.delegate = delegate;
            this.admin = admin;
            this.healthy = healthy;
        }

        @Override
        public boolean healthy() {
            return healthy.getAsBoolean();
        }

        @Override
        public boolean awaitCaughtUp() {
            return delegate.awaitCaughtUp();
        }

        @Override
        public void close() {
            admin.close();
        }
    }

    @Bean
    public SmartLifecycle matchingSlotMapReaderLifecycle(
            MatchingSlotMapReader reader, ObjectProvider<KafkaMatchingShardAssignment> assignment) {
        return new HandoverLifecycle(MAP_READER_PHASE,
            () -> {
                // 맵 토픽이 있어야 읽을 수 있다. 토픽 만들기는 배정 생명주기의 시작(-2)에서 하므로 읽기를 시작하기 전에 먼저 한다.
                assignment.getObject().prepareTopics();
                reader.start();
                reader.awaitInitialCatchUp(Duration.ofSeconds(30));
            },
            reader::close);
    }

    /**
     * 넘긴 쪽(X)의 재시작 규칙(U16c)을 엔진 시작 전에 적용한다. 맵 읽기(phase -3)가 따라잡은 뒤, 엔진(phase 0)이 시작되기 전에
     * 돌도록 phase -1에 둔다. 기다리는 시간은 {@code matching-shard.recovery.map-catch-up-timeout-ms}(기본 10초)이고 넘기면
     * 호가창을 지우지 않고 경고만 남긴다.
     */
    @Bean
    public SmartLifecycle handoverRestartRuleLifecycle(
            MatchingSlotMapReader reader, HandoverBookStore store, ObjectProvider<MatchingEngine> engine,
            ObjectProvider<Optional<StoredMatchingSnapshot>> loadedSnapshot,
            @Qualifier("matchingJournalRecoveredEntries") ObjectProvider<List<JournaledOrder>> matchingJournalRecoveredEntries,
            @Value("${matching-shard.recovery.map-catch-up-timeout-ms:10000}") long timeoutMillis,
            @Value("${transport.matching-intake.channel:" + MatchingOrderIntakeConfig.DEFAULT_INTAKE_CHANNEL + "}") String ownChannel) {
        HandoverRestartRule rule = new HandoverRestartRule(ownChannel, reader::targetFor, store);
        return new HandoverLifecycle(RESTART_RULE_PHASE,
            () -> {
                MatchingEngine matchingEngine = engine.getIfAvailable();
                List<JournaledOrder> journalEntries = matchingJournalRecoveredEntries.getIfAvailable();
                if (matchingEngine == null || journalEntries == null) {
                    return;
                }
                Set<Integer> snapshotSlots = loadedSnapshot.getIfAvailable(Optional::empty)
                    .map(StoredMatchingSnapshot::ownedStockSlots).orElse(Set.of());
                Set<Integer> ownedSlots = OwnedStockSlotRestorer.restoreFromJournal(snapshotSlots, journalEntries);
                // 엔진도 같은 목록을 알아야 담당 중인 종목 slot에 도착한 낡은 호가창 적재를 막는다(스냅샷 이전에 받은 종목 slot 포함).
                matchingEngine.seedAcquiredSlots(ownedSlots);
                rule.applyWhenMapReady(() -> reader.awaitInitialCatchUp(Duration.ofMillis(timeoutMillis)), matchingEngine, ownedSlots);
            },
            () -> { });
    }

    @Bean
    public HandoverResponder handoverResponder(
            Aeron aeron, HandoverEndpointPool pool, HandoverBookStore store,
            @Value("${transport.matching-intake.channel:" + MatchingOrderIntakeConfig.DEFAULT_INTAKE_CHANNEL + "}") String ownChannel) {
        return new HandoverResponder(aeron, ownChannel, pool, store);
    }

    @Bean
    public HandoverRequester handoverRequester(
            Aeron aeron, HandoverEndpointPool pool, MatchingSlotMapReader reader,
            @Value("${matching-worker.instance-id:}") String instanceId,
            @Value("${transport.matching-intake.channel:" + MatchingOrderIntakeConfig.DEFAULT_INTAKE_CHANNEL + "}") String ownChannel) {
        long nodeId = SnowflakeConfig.nodeIdFromInstanceId(instanceId);
        return new HandoverRequester(aeron, ownChannel, pool, reader::targetFor, nodeId);
    }

    @Bean
    public HandoverReceiver handoverReceiver(
            Aeron aeron, HandoverRequester requester, SlotEventRequests slotEventRequests,
            @Value("${transport.matching-intake.channel:" + MatchingOrderIntakeConfig.DEFAULT_INTAKE_CHANNEL + "}") String ownChannel) {
        return new HandoverReceiver(aeron, ownChannel, requester, slotEventRequests::requestLoad, slotEventRequests::requestCleanup);
    }

    @Bean
    public HandoverCoordinator handoverCoordinator(
            HandoverRequester requester,
            @Value("${matching-shard.handover.retry-interval-ms:1000}") long retryIntervalMillis,
            @Value("${matching-shard.handover.response-timeout-ms:5000}") long responseTimeoutMillis,
            @Value("${matching-shard.handover.abandoned-retry-interval-ms:30000}") long abandonedRetryIntervalMillis) {
        // 기본값은 Jack이 정한 값: 응답이 오지 않으면 1초마다 같은 요청을 다시 보내고, 처음 요청부터 5초가 지나면 실패로 본다.
        // 포기 뒤 다시 요청하는 간격 기본값 30초는 근거 없는 임의 값이다(미측정). X가 일시적으로 응답하지 못하는 시간을 측정한 뒤 정한다.
        return new HandoverCoordinator(requester, retryIntervalMillis, responseTimeoutMillis, abandonedRetryIntervalMillis);
    }

    /**
     * 이동이 정한 시간 넘게 멈춘 종목 slot을 찾아 ERROR 신호로 남기고 조회할 수 있게 한다. 대기 상태를 가진 담당 배정이 있을 때만 만든다.
     * 값은 모두 근거 없는 임의 값이다(미측정): 30초 넘게 머물면 멈춘 것으로 보고, 같은 종목 slot의 신호는 60초에 한 번, 5초마다 확인한다.
     */
    @Bean(destroyMethod = "close")
    public StuckHandoverReporterRunner stuckHandoverReporter(
            ObjectProvider<KafkaMatchingShardAssignment> assignment, ObjectProvider<HandoverCoordinator> coordinator,
            @Value("${matching-shard.handover.stuck-threshold-ms:30000}") long thresholdMillis,
            @Value("${matching-shard.handover.stuck-repeat-ms:60000}") long repeatMillis,
            @Value("${matching-shard.handover.stuck-check-interval-ms:5000}") long checkIntervalMillis) {
        StuckHandoverReporter reporter = new StuckHandoverReporter(
            () -> {
                KafkaMatchingShardAssignment current = assignment.getIfAvailable();
                return current == null ? java.util.Set.of() : current.slotsWaitingForMove();
            },
            () -> {
                HandoverCoordinator current = coordinator.getIfAvailable();
                return current == null ? 0L : current.failures();
            },
            System::currentTimeMillis, thresholdMillis, repeatMillis);
        return new StuckHandoverReporterRunner(reporter, checkIntervalMillis);
    }

    /** 멈춤 신호를 정한 간격마다 확인하는 예약 실행. 신호 조회는 {@link #stuckHandoverSlots}로 한다. */
    public static final class StuckHandoverReporterRunner implements AutoCloseable {
        private final StuckHandoverReporter reporter;
        private final java.util.concurrent.ScheduledExecutorService executor =
            java.util.concurrent.Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "stuck-handover-reporter");
                thread.setDaemon(true);
                return thread;
            });

        StuckHandoverReporterRunner(StuckHandoverReporter reporter, long checkIntervalMillis) {
            this.reporter = reporter;
            executor.scheduleWithFixedDelay(() -> {
                try {
                    reporter.check();
                } catch (RuntimeException e) {
                    System.getLogger(StuckHandoverReporterRunner.class.getName())
                        .log(System.Logger.Level.ERROR, "[호가창 이동] 멈춤 확인 중 예외(계속함): " + e);
                }
            }, checkIntervalMillis, checkIntervalMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
        }

        /** 정한 시간 넘게 이동이 멈춘 종목 slot. */
        public java.util.Set<Integer> stuckHandoverSlots() {
            return reporter.stuckHandoverSlots();
        }

        @Override
        public void close() {
            executor.shutdownNow();
        }
    }

    @Bean
    public SmartLifecycle handoverTransportLifecycle(
            HandoverResponder responder, HandoverReceiver receiver, HandoverRequester requester,
            HandoverCoordinator coordinator) {
        return new HandoverLifecycle(TRANSPORT_PHASE,
            () -> {
                responder.start();
                receiver.start();
                coordinator.start();
            },
            () -> {
                coordinator.close();
                receiver.close();
                responder.close();
                requester.close();
            });
    }
}
