package com.flab.stocktradingengine.account.worker.config;

import java.util.Properties;
import java.util.Set;
import java.util.function.IntPredicate;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.account.worker.coordination.KafkaShardAssignment;
import com.flab.stocktradingengine.aeron.WorkerEndpoints;
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
     * 이 빈이 담당 슬롯 판정({@link IntPredicate})을 겸한다 — {@link KafkaShardAssignment}가
     * {@link IntPredicate}를 직접 구현하므로 {@link AccountEngineConfig}의 주입 자리에 타입으로
     * 그대로 맞는다. 판정용 빈을 따로 한 겹 더 두면 {@link IntPredicate} 후보가 둘이 되어
     * (이 빈 + 그 빈) Spring이 어느 것을 넣을지 정하지 못하고 기동이 멈춘다.
     *
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
            @Value("${transport.account-intake.channel:" + AccountOrderIntakeConfig.DEFAULT_INTAKE_CHANNEL + "}") String orderEndpoint,
            @Value("${transport.fill.channel:" + AccountFillIntakeConfig.DEFAULT_FILL_CHANNEL + "}") String fillEndpoint,
            @Value("${account-shard.coordination.session-timeout-ms:120000}") long sessionTimeoutMs) {
        String instanceId = resolveInstanceId(instanceIdConfig, orderEndpoint);
        log.info("[계좌] 담당 슬롯 소스: Kafka 컨슈머 그룹 배정({}) — 워커 신원={} 주문 수신={} 체결 수신={} session.timeout.ms={}",
            KafkaShardAssignment.GROUP_ID, instanceId, orderEndpoint, fillEndpoint, sessionTimeoutMs);
        Consumer<String, String> consumer = buildConsumer(bootstrapServers, instanceId, sessionTimeoutMs);
        Producer<Integer, String> mapProducer = buildMapProducer(bootstrapServers);
        Admin adminClient = Admin.create(adminProps(bootstrapServers));
        // account-shard-map에 싣는 값은 이 워커가 실제로 듣고 있는 Aeron 주소 둘이다 — 주문을
        // 받는 채널과 체결을 받는 채널이 다르기 때문에 둘 다 싣는다. group.instance.id(워커 신원,
        // k8s pod 이름 등 임의 문자열일 수 있음)와는 다른 값이다.
        return new KafkaShardAssignment(consumer, mapProducer, adminClient, shardRoutingProperties.slotCount(),
            new WorkerEndpoints(orderEndpoint, fillEndpoint));
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

    private Producer<Integer, String> buildMapProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 슬롯당 최신 값 하나만 의미 있다(compacted) — 순서 역전을 막으려 레코드마다 즉시 get()으로
        // 확인하므로(KafkaShardAssignment#publishMapEntries) 재시도 중 중복 발행이 net effect를
        // 안 바꾼다. acks=all로 durable하게 남긴다.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    private Properties adminProps(String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return props;
    }
}
