package com.flab.stocktradingengine.account.worker.config;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.aeron.ShardRoutingTable;
import com.flab.stocktradingengine.aeron.SlotHasher;

import lombok.extern.slf4j.Slf4j;

/**
 * I8 U2 — {@link ShardRoutingProperties}를 {@link ShardRoutingTable}로 바꿔 빈으로 등록하고,
 * 이 계좌 워커 자신이 어느 슬롯 집합을 담당하는지(D2, 계좌 샤딩 U3) 정한다.
 *
 * <p>담당 슬롯을 따로 적는 프로퍼티를 새로 만들지 않는다 — 이 워커의 인테이크 채널({@code
 * transport.account-intake.channel}, api가 fan-out 목적지로 쓰는 것과 같은 값)과 일치하는 슬롯
 * 범위를 표에서 찾아 "그게 내 담당"으로 삼는다. 담당을 적는 사실이 두 곳(표·별도 프로퍼티)에
 * 각자 있으면 어긋날 수 있다 — 표 한 곳만 진실이다.</p>
 *
 * <p>담당을 endpoint 문자열이 아니라 슬롯 번호 {@link Set}으로 들고 있다(계좌 샤딩 U3) — U4에서
 * Kafka 컨슈머 그룹이 배정하는 것도 슬롯(파티션 번호) 집합이라, 배정 소스가 바뀌어도 이 워커가
 * 소비하는 형태(슬롯 집합)는 같게 맞춘다.</p>
 */
@Slf4j
@Configuration
@EnableConfigurationProperties(ShardRoutingProperties.class)
public class ShardRoutingConfig {

    /**
     * 계좌번호를 칸 번호로 바꾸는 계산기. 조정 모드에서는 칸 개수를 {@code shard-routing.slot-count}로
     * 잡는다 — 조정 토픽을 이 개수로 만드는 것이 계좌 워커이고({@code KafkaShardAssignment}가 기동할
     * 때 만들고, 이미 있으면 파티션 개수가 이 값과 같은지 검사한다), api·매칭 워커는 그렇게 만들어진
     * 토픽의 파티션 개수를 읽어 같은 값을 쓴다.
     *
     * <p>정적 모드에서 담당 범위 표가 없으면 칸 1개짜리로 둔다 — 샤딩을 안 쓰던 예전 동작이다.</p>
     */
    @Bean
    public SlotHasher slotHasher(
            ShardRoutingProperties properties,
            @Value("${account-shard.coordination.enabled:false}") boolean coordinationEnabled) {
        if (coordinationEnabled) {
            log.info("[계좌] 칸 계산: 칸 {}개(shard-routing.slot-count, 조정 토픽도 이 개수로 만든다)", properties.slotCount());
            return new SlotHasher(properties.slotCount());
        }
        if (properties.shards().isEmpty()) {
            log.info("[계좌] 칸 계산: 칸 1개(샤딩 미설정) — 모든 계좌를 이 워커가 담당한다");
            return new SlotHasher(1);
        }
        log.info("[계좌] 칸 계산: 칸 {}개(shard-routing.slot-count)", properties.slotCount());
        return new SlotHasher(properties.slotCount());
    }

    @Bean
    public ShardRoutingTable shardRoutingTable(
            ShardRoutingProperties properties,
            @Value("${transport.account-intake.channel:" + AccountOrderIntakeConfig.DEFAULT_INTAKE_CHANNEL + "}") String defaultAccountIntakeChannel) {
        if (properties.shards().isEmpty()) {
            log.info("[계좌] shard-routing.shards 미설정 — 단일 목적지로 폴백: channel={}", defaultAccountIntakeChannel);
            return new ShardRoutingTable(1, List.of(new ShardRoutingTable.ShardRange(defaultAccountIntakeChannel, 0, 0)));
        }
        List<ShardRoutingTable.ShardRange> ranges = properties.shards().stream()
            .map(shard -> new ShardRoutingTable.ShardRange(shard.endpoint(), shard.slotFrom(), shard.slotTo()))
            .toList();
        log.info("[계좌] shard-routing.shards {}건 로드 — slotCount={} 목적지={}",
            ranges.size(), properties.slotCount(), ranges);
        return new ShardRoutingTable(properties.slotCount(), ranges);
    }

    /**
     * 이 워커의 인테이크 채널과 일치하는 슬롯 범위를 찾아 그 범위의 슬롯 번호 전부를 담당으로
     * 확정한다. 없으면(설정이 어긋난 상태) 기동을 멈춘다 — 모든 주문이 NOT_OWNED로 거부되는
     * 채로 뜨는 것보다 낫다.
     */
    @Bean
    public OwnedShard ownedShard(
            ShardRoutingProperties properties,
            @Value("${transport.account-intake.channel:" + AccountOrderIntakeConfig.DEFAULT_INTAKE_CHANNEL + "}") String ownEndpoint) {
        if (properties.shards().isEmpty()) {
            log.info("[계좌] 담당 슬롯: 전체(샤딩 미설정) endpoint={}", ownEndpoint);
            return new OwnedShard(Set.of(0));
        }
        Optional<ShardRoutingProperties.ShardConfig> owned = properties.shards().stream()
            .filter(shard -> shard.endpoint().equals(ownEndpoint))
            .findFirst();
        if (owned.isEmpty()) {
            throw new IllegalStateException(
                "이 계좌 워커의 인테이크 채널이 shard-routing.shards 표에 없습니다(설정이 어긋났습니다): endpoint="
                    + ownEndpoint + " 표의 목적지=" + properties.shards().stream().map(ShardRoutingProperties.ShardConfig::endpoint).toList());
        }
        Set<Integer> ownedSlots = IntStream.rangeClosed(owned.get().slotFrom(), owned.get().slotTo())
            .boxed()
            .collect(java.util.stream.Collectors.toUnmodifiableSet());
        log.info("[계좌] 담당 슬롯: endpoint={} slot=[{}, {}] (전체 slotCount={})",
            ownEndpoint, owned.get().slotFrom(), owned.get().slotTo(), properties.slotCount());
        return new OwnedShard(ownedSlots);
    }

    /** 이 워커가 담당하는 슬롯 번호 집합. {@link ShardRoutingTable#slotFor}가 이 집합에 있으면 담당이다. */
    public record OwnedShard(Set<Integer> slots) {
    }
}
