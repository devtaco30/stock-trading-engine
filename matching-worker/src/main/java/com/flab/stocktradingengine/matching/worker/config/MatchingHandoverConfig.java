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
        return new HandoverReceiver(aeron, ownChannel, requester, slotEventRequests::requestLoad);
    }

    @Bean
    public HandoverCoordinator handoverCoordinator(HandoverRequester requester) {
        return new HandoverCoordinator(requester::request);
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
