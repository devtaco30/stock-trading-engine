package com.flab.stocktradingengine.matching.worker.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * fork3, Unit 1 — 매칭(sender)이 체결 fan-out 목적지를 정하는 정적 샤드 라우팅 설정.
 * {@code shard-routing.slot-count}(ADR-031 고정값 256)와 슬롯 범위별 목적지 endpoint 목록.
 */
@ConfigurationProperties(prefix = "shard-routing")
public record ShardRoutingProperties(int slotCount, List<ShardConfig> shards) {

    /**
     * {@code shard-routing.shards}가 아예 안 주어지면(샤딩 미도입 환경, 기존 통합테스트들처럼
     * 이 설정을 모르는 컨텍스트) 바인더가 null을 준다. null이면 {@link ShardRoutingConfig}가
     * 바로 for-each/stream 해 기동 자체가 NPE로 죽으므로, 빈 리스트로 정규화해 "샤딩 미설정"과
     * "shards 있음"을 {@link ShardRoutingConfig}가 안전하게 구분할 수 있게 한다.
     */
    public ShardRoutingProperties {
        shards = shards == null ? List.of() : shards;
    }

    public record ShardConfig(String endpoint, int slotFrom, int slotTo) {
    }
}
