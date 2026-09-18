package com.flab.stocktradingengine.api.config;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.aeron.AccountDestinationResolver;
import com.flab.stocktradingengine.aeron.AssignmentDestinationResolver;
import com.flab.stocktradingengine.aeron.ShardRoutingTable;
import com.flab.stocktradingengine.aeron.ShardSlotCountSource;
import com.flab.stocktradingengine.aeron.SlotHasher;
import com.flab.stocktradingengine.aeron.StaticShardDestinationResolver;
import com.flab.stocktradingengine.api.lifecycle.AssignmentDestinationResolverLifecycle;

import lombok.extern.slf4j.Slf4j;

/**
 * I8 U1 — {@link ShardRoutingProperties}를 {@link ShardRoutingTable}로 바꿔 빈으로 등록한다.
 * matching-worker의 같은 이름 클래스를 그대로 미러한다(생성자가 슬롯 커버리지를 검증해 설정이
 * 잘못되면 이 빈 생성 시점에 기동이 실패한다 — fail-fast).
 *
 * <p>{@code shard-routing.shards}가 아예 없으면(샤딩 미도입 환경 — 기존 udp/e2e 프로파일처럼
 * {@code transport.account-intake.channel} 하나로만 돌던 통합테스트 전부) 슬롯 1개짜리 기본
 * 테이블로 폴백해 그 채널 하나로만 라우팅한다.</p>
 */
@Slf4j
@Configuration
@EnableConfigurationProperties(ShardRoutingProperties.class)
public class ShardRoutingConfig {

    /**
     * 정적 모드의 칸 계산기 — 담당 범위 표에 적힌 칸 개수를 그대로 쓴다. 표가 없으면(샤딩
     * 미도입 환경) 칸 1개짜리로 둬서 모든 계좌가 같은 목적지로 가던 예전 동작을 유지한다.
     */
    @Bean
    @ConditionalOnProperty(prefix = "account-shard.coordination", name = "enabled", havingValue = "false", matchIfMissing = true)
    public SlotHasher staticSlotHasher(ShardRoutingProperties properties) {
        if (properties.shards().isEmpty()) {
            log.info("[api] 칸 계산: 칸 1개(샤딩 미설정) — 모든 계좌가 같은 목적지로 간다");
            return new SlotHasher(1);
        }
        log.info("[api] 칸 계산: 칸 {}개(shard-routing.slot-count)", properties.slotCount());
        return new SlotHasher(properties.slotCount());
    }

    /**
     * 조정 모드의 칸 계산기 — 칸 개수를 설정 파일이 아니라 조정 토픽의 파티션 개수에서 읽는다.
     * 앱마다 같은 숫자를 사람이 적어 맞추면 한 곳만 틀려도 주문이 틀린 워커로 가기 때문이다
     * ({@link ShardSlotCountSource}).
     */
    @Bean
    @ConditionalOnProperty(prefix = "account-shard.coordination", name = "enabled", havingValue = "true")
    public SlotHasher coordinationSlotHasher(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (Admin admin = Admin.create(props)) {
            int slotCount = ShardSlotCountSource.readSlotCount(admin, SLOT_COUNT_WAIT);
            log.info("[api] 칸 계산: 칸 {}개 — 조정 토픽 {}의 파티션 개수에서 읽었다",
                slotCount, ShardSlotCountSource.ASSIGNMENT_TOPIC);
            return new SlotHasher(slotCount);
        }
    }

    /** 조정 토픽이 생기기를 기다리는 시간. 계좌 워커가 만들기 전에 이 앱이 먼저 뜰 수 있다. */
    private static final Duration SLOT_COUNT_WAIT = Duration.ofSeconds(60);

    static final String DEFAULT_ACCOUNT_INTAKE_CHANNEL = "aeron:ipc";

    /**
     * 어느 갈래로 떴는지를 기동 로그에 남긴다 — 측정할 때 "설정한 shard-routing이 실제로
     * 먹었나"를 이 줄 하나로 확인할 수 있게 한다. 조용히 폴백해 있으면 나중에 "왜 한쪽으로만
     * 가지"를 추적할 방법이 없다.
     */
    @Bean
    public ShardRoutingTable shardRoutingTable(
            ShardRoutingProperties properties,
            @Value("${transport.account-intake.channel:" + DEFAULT_ACCOUNT_INTAKE_CHANNEL + "}") String defaultAccountIntakeChannel) {
        if (properties.shards().isEmpty()) {
            log.info("[api] shard-routing.shards 미설정 — 단일 목적지로 폴백: channel={}", defaultAccountIntakeChannel);
            return new ShardRoutingTable(1, List.of(new ShardRoutingTable.ShardRange(defaultAccountIntakeChannel, 0, 0)));
        }
        List<ShardRoutingTable.ShardRange> ranges = properties.shards().stream()
            .map(shard -> new ShardRoutingTable.ShardRange(shard.endpoint(), shard.slotFrom(), shard.slotTo()))
            .toList();
        log.info("[api] shard-routing.shards {}건 로드 — slotCount={} 목적지={}",
            ranges.size(), properties.slotCount(), ranges);
        return new ShardRoutingTable(properties.slotCount(), ranges);
    }

    /**
     * 계좌 샤딩 U2·U5 — 보내는 쪽({@link com.flab.stocktradingengine.api.messaging.AeronAccountOrderSender})은
     * 이 인터페이스에만 의존한다. {@code account-shard.coordination.enabled}로 정적(U2)·동적(U5)
     * 구현 중 정확히 하나만 등록한다 — 둘 다 살면 어느 쪽이 실제 목적지를 정했는지 추적할 수
     * 없어진다(account-worker의 같은 원칙, {@code AccountShardOwnershipConfig} 참고).
     */
    @Bean
    @ConditionalOnProperty(prefix = "account-shard.coordination", name = "enabled", havingValue = "false", matchIfMissing = true)
    public AccountDestinationResolver staticAccountDestinationResolver(ShardRoutingTable shardRoutingTable) {
        return new StaticShardDestinationResolver(shardRoutingTable);
    }

    /**
     * account-shard-map(계좌 워커들이 배정받은 슬롯마다 자기 endpoint를 발행하는 compacted 토픽)을
     * 구독해 목적지 표를 실시간으로 유지한다. 컨슈머 그룹이 아니라(api 인스턴스마다 표 전체가
     * 다 있어야 한다) 전체를 직접 맡아 처음부터 읽는다({@link AssignmentDestinationResolver}).
     */
    /**
     * 이 빈이 목적지 판정({@link AccountDestinationResolver})을 겸한다 — {@link
     * AssignmentDestinationResolver}가 그 인터페이스를 직접 구현하므로 주입 자리에 타입으로
     * 그대로 맞는다. 판정용 빈을 따로 한 겹 더 두면 후보가 둘이 되어 Spring이 어느 것을 넣을지
     * 정하지 못하고 기동이 멈춘다.
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "account-shard.coordination", name = "enabled", havingValue = "true")
    public AssignmentDestinationResolver assignmentDestinationResolver(
            SlotHasher slotHasher,
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        log.info("[api] 목적지 소스: account-shard-map 구독(동적) — 칸 {}개", slotHasher.slotCount());
        Consumer<Integer, String> consumer = buildMapConsumer(bootstrapServers);
        return new AssignmentDestinationResolver(consumer, slotHasher);
    }


    @Bean
    @ConditionalOnProperty(prefix = "account-shard.coordination", name = "enabled", havingValue = "true")
    public SmartLifecycle assignmentDestinationResolverLifecycle(AssignmentDestinationResolver assignmentDestinationResolver) {
        return new AssignmentDestinationResolverLifecycle(assignmentDestinationResolver);
    }

    private Consumer<Integer, String> buildMapConsumer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }
}
