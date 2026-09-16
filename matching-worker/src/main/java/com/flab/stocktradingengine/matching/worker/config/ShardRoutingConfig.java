package com.flab.stocktradingengine.matching.worker.config;

import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.aeron.ShardRoutingTable;

import lombok.extern.slf4j.Slf4j;

/**
 * fork3, Unit 1 — {@link ShardRoutingProperties}를 {@link ShardRoutingTable}로 바꿔 빈으로 등록한다.
 * {@link ShardRoutingTable} 생성자가 슬롯 커버리지(갭·중복 0)를 검증하므로, 설정이 잘못되면 이 빈
 * 생성 시점에 애플리케이션 기동이 실패한다(fail-fast).
 *
 * <p>{@code shard-routing.shards}가 아예 없으면(샤딩 미도입 환경 — fork1까지의 단일 워커 배선,
 * 그리고 이 설정을 모르는 기존 통합테스트 전부) 슬롯 1개짜리 기본 테이블로 폴백해 기존
 * {@code transport.fill.channel} 하나로만 라우팅한다 — {@link MatchingFillPublishConfig}의
 * {@code transport.*} 미설정 시 {@code aeron:ipc} 기본값 패턴과 같은 결이다. 스펙엔 이 폴백이
 * 명시돼 있지 않아 직접 판단해 넣었다(리뷰 시 플래그).</p>
 */
@Slf4j
@Configuration
@EnableConfigurationProperties(ShardRoutingProperties.class)
public class ShardRoutingConfig {

    static final String DEFAULT_FILL_CHANNEL = "aeron:ipc";

    /**
     * 어느 갈래로 떴는지를 기동 로그에 남긴다(I8 U3 — api·계좌워커의 같은 클래스(D2-b)에는 이미
     * 있었는데 이 클래스에만 없어서, 2워커 측정 때 "매칭 쪽 설정이 실제로 먹었나"를 로그로 못 보고
     * 체결 데이터를 손으로 추적해 확인해야 했다). 동작은 그대로다 — 로그 한 줄만 추가한다.
     */
    @Bean
    public ShardRoutingTable shardRoutingTable(
            ShardRoutingProperties properties,
            @Value("${transport.fill.channel:" + DEFAULT_FILL_CHANNEL + "}") String defaultFillChannel) {
        if (properties.shards().isEmpty()) {
            log.info("[매칭] shard-routing.shards 미설정 — 단일 목적지로 폴백: channel={}", defaultFillChannel);
            return new ShardRoutingTable(1, List.of(new ShardRoutingTable.ShardRange(defaultFillChannel, 0, 0)));
        }
        List<ShardRoutingTable.ShardRange> ranges = properties.shards().stream()
            .map(shard -> new ShardRoutingTable.ShardRange(shard.endpoint(), shard.slotFrom(), shard.slotTo()))
            .toList();
        log.info("[매칭] shard-routing.shards {}건 로드 — slotCount={} 목적지={}",
            ranges.size(), properties.slotCount(), ranges);
        return new ShardRoutingTable(properties.slotCount(), ranges);
    }
}
