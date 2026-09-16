package com.flab.stocktradingengine.api.config;

import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.aeron.ShardRoutingTable;

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
}
