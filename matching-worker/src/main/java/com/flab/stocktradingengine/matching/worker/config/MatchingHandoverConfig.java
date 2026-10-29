package com.flab.stocktradingengine.matching.worker.config;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import com.flab.stocktradingengine.aeron.StockSlotHasher;
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventRequests;
import com.flab.stocktradingengine.matching.worker.coordination.HandoverEndpointPool;
import com.flab.stocktradingengine.matching.worker.coordination.KafkaMatchingShardAssignment;
import com.flab.stocktradingengine.matching.worker.coordination.MatchingSlotMapReader;
import com.flab.stocktradingengine.matching.worker.handover.HandoverBookStore;
import com.flab.stocktradingengine.matching.worker.handover.HandoverCoordinator;
import com.flab.stocktradingengine.matching.worker.handover.HandoverReceiver;
import com.flab.stocktradingengine.matching.worker.handover.HandoverRequester;
import com.flab.stocktradingengine.matching.worker.handover.HandoverResponder;
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
    // 요청 응답·수신 thread는 엔진(0)과 주문 수신(1)이 돈 뒤에 시작한다.
    private static final int TRANSPORT_PHASE = 2;

    @Bean
    public HandoverEndpointPool handoverEndpointPool(
            Environment environment,
            @Value("${transport.matching-intake.channel:" + MatchingOrderIntakeConfig.DEFAULT_INTAKE_CHANNEL + "}") String ownChannel) {
        List<String> endpoints = Binder.get(environment)
            .bind("matching-shard.handover.endpoints", Bindable.listOf(String.class)).orElse(List.of());
        return new HandoverEndpointPool(endpoints, ownChannel);
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

    @Bean
    public SmartLifecycle matchingSlotMapReaderLifecycle(MatchingSlotMapReader reader) {
        return new HandoverLifecycle(MAP_READER_PHASE,
            () -> {
                reader.start();
                reader.awaitInitialCatchUp(Duration.ofSeconds(30));
            },
            reader::close);
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
