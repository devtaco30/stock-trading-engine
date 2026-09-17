package com.flab.stocktradingengine.account.worker.config;

import java.util.Properties;
import java.util.Set;
import java.util.function.IntPredicate;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.account.worker.coordination.KafkaShardAssignment;
import com.flab.stocktradingengine.account.worker.lifecycle.KafkaShardAssignmentLifecycle;

import lombok.extern.slf4j.Slf4j;

/**
 * 계좌 샤딩 U4 — 담당 슬롯 판정({@link IntPredicate})의 소스를 정적 설정(U3, {@link
 * ShardRoutingConfig})과 Kafka 컨슈머 그룹 배정({@link KafkaShardAssignment}) 중 정확히 하나로
 * 고정한다.
 *
 * <p>{@code account-shard.coordination.enabled}가 이 둘을 가르는 유일한 스위치다 — 둘 다 빈으로
 * 살아 있으면 어느 쪽이 실제 주인을 정했는지 나중에 추적할 수 없어진다(라우팅 사고 원인 추적
 * 불가). 그래서 {@link ConditionalOnProperty}로 반드시 배타적으로 하나만 등록한다. 기본값은
 * false(정적, U3까지의 기존 동작) — 기존 실행 구성(로컬 3-JVM, e2e 스크립트)이 안 깨진다.</p>
 */
@Slf4j
@Configuration
public class AccountShardOwnershipConfig {

    @Bean
    @ConditionalOnProperty(prefix = "account-shard.coordination", name = "enabled", havingValue = "false", matchIfMissing = true)
    public IntPredicate staticOwnedSlots(ShardRoutingConfig.OwnedShard ownedShard) {
        Set<Integer> slots = ownedShard.slots();
        log.info("[계좌] 담당 슬롯 소스: 정적 설정(shard-routing.shards) — 슬롯={}", slots);
        return slots::contains;
    }

    /**
     * @param sessionTimeoutMs 기본값 120000(2분)은 잠정값이다 — LLD U7이 "워커 재시작 시간(프로세스
     *     시작부터 저널 재생을 마치고 주문을 받기까지)"을 실측한 뒤 그 값으로 다시 정한다. 그
     *     전까지는 기본 45000보다 넉넉히 길게 잡아, 정상 재시작 중에 슬롯이 남에게 넘어가는(L1 위반)
     *     사고를 피하는 안전 쪽으로 둔 값이다.
     */
    @Bean
    @ConditionalOnProperty(prefix = "account-shard.coordination", name = "enabled", havingValue = "true")
    public KafkaShardAssignment kafkaShardAssignment(
            ShardRoutingProperties shardRoutingProperties,
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${account-worker.instance-id:}") String instanceIdConfig,
            @Value("${transport.account-intake.channel:" + AccountOrderIntakeConfig.DEFAULT_INTAKE_CHANNEL + "}") String fallbackEndpoint,
            @Value("${account-shard.coordination.session-timeout-ms:120000}") long sessionTimeoutMs) {
        String instanceId = resolveInstanceId(instanceIdConfig, fallbackEndpoint);
        log.info("[계좌] 담당 슬롯 소스: Kafka 컨슈머 그룹 배정({}) — 워커 신원={} session.timeout.ms={}",
            KafkaShardAssignment.GROUP_ID, instanceId, sessionTimeoutMs);
        Consumer<String, String> consumer = buildConsumer(bootstrapServers, instanceId, sessionTimeoutMs);
        Admin adminClient = Admin.create(adminProps(bootstrapServers));
        return new KafkaShardAssignment(consumer, adminClient, shardRoutingProperties.slotCount());
    }

    @Bean
    @ConditionalOnProperty(prefix = "account-shard.coordination", name = "enabled", havingValue = "true")
    public IntPredicate kafkaOwnedSlots(KafkaShardAssignment kafkaShardAssignment) {
        return kafkaShardAssignment;
    }

    @Bean
    @ConditionalOnProperty(prefix = "account-shard.coordination", name = "enabled", havingValue = "true")
    public SmartLifecycle kafkaShardAssignmentLifecycle(KafkaShardAssignment kafkaShardAssignment) {
        return new KafkaShardAssignmentLifecycle(kafkaShardAssignment);
    }

    /**
     * {@code account-worker.instance-id}가 이 워커의 static membership 신원(group.instance.id)이다.
     * 안 주면 자기 인테이크 endpoint로 폴백한다 — 로컬 3-JVM 실행(20040·20041 등)에서는 워커마다
     * endpoint가 달라 폴백만으로도 자동 구분된다. k8s에서는 StatefulSet pod 이름(account-worker-0
     * 등)을 이 키에 넣으면 재시작해도 같은 신원을 유지한다(ADR-031이 요구하는 안정 신원).
     */
    private String resolveInstanceId(String instanceIdConfig, String fallbackEndpoint) {
        if (instanceIdConfig != null && !instanceIdConfig.isBlank()) {
            return instanceIdConfig;
        }
        log.warn("[계좌] account-worker.instance-id 미설정 — transport.account-intake.channel로 폴백: endpoint={}", fallbackEndpoint);
        return fallbackEndpoint;
    }

    private Consumer<String, String> buildConsumer(String bootstrapServers, String instanceId, long sessionTimeoutMs) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaShardAssignment.GROUP_ID);
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, (int) sessionTimeoutMs);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }

    private Properties adminProps(String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return props;
    }
}
