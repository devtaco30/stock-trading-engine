package com.flab.stocktradingengine.account.worker.config;

import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.aeron.ShardRoutingTable;

import lombok.extern.slf4j.Slf4j;

/**
 * I8 U2 — {@link ShardRoutingProperties}를 {@link ShardRoutingTable}로 바꿔 빈으로 등록하고,
 * 이 계좌 워커 자신이 어느 슬롯을 담당하는지(D2) 정한다.
 *
 * <p>담당 슬롯을 따로 적는 프로퍼티를 새로 만들지 않는다 — 이 워커의 인테이크 채널({@code
 * transport.account-intake.channel}, api가 fan-out 목적지로 쓰는 것과 같은 값)과 일치하는 슬롯
 * 범위를 표에서 찾아 "그게 내 담당"으로 삼는다. 담당을 적는 사실이 두 곳(표·별도 프로퍼티)에
 * 각자 있으면 어긋날 수 있다 — 표 한 곳만 진실이다.</p>
 */
@Slf4j
@Configuration
@EnableConfigurationProperties(ShardRoutingProperties.class)
public class ShardRoutingConfig {

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
     * 이 워커의 인테이크 채널과 일치하는 슬롯 범위를 찾아 담당으로 확정한다. 없으면(설정이
     * 어긋난 상태) 기동을 멈춘다 — 모든 주문이 NOT_OWNED로 거부되는 채로 뜨는 것보다 낫다.
     */
    @Bean
    public OwnedShard ownedShard(
            ShardRoutingProperties properties,
            @Value("${transport.account-intake.channel:" + AccountOrderIntakeConfig.DEFAULT_INTAKE_CHANNEL + "}") String ownEndpoint) {
        if (properties.shards().isEmpty()) {
            log.info("[계좌] 담당 슬롯: 전체(샤딩 미설정) endpoint={}", ownEndpoint);
            return new OwnedShard(ownEndpoint);
        }
        Optional<ShardRoutingProperties.ShardConfig> owned = properties.shards().stream()
            .filter(shard -> shard.endpoint().equals(ownEndpoint))
            .findFirst();
        if (owned.isEmpty()) {
            throw new IllegalStateException(
                "이 계좌 워커의 인테이크 채널이 shard-routing.shards 표에 없습니다(설정이 어긋났습니다): endpoint="
                    + ownEndpoint + " 표의 목적지=" + properties.shards().stream().map(ShardRoutingProperties.ShardConfig::endpoint).toList());
        }
        log.info("[계좌] 담당 슬롯: endpoint={} slot=[{}, {}] (전체 slotCount={})",
            ownEndpoint, owned.get().slotFrom(), owned.get().slotTo(), properties.slotCount());
        return new OwnedShard(ownEndpoint);
    }

    /** 이 워커 자신의 endpoint. {@link ShardRoutingTable}에서 이 값과 일치하는 범위가 담당 슬롯이다. */
    public record OwnedShard(String endpoint) {
    }
}
