package com.flab.stocktradingengine.account.worker.config;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 매칭 샤딩 U9 — 계좌 shard가 주문을 보낼 수 있는 매칭 pod의 주소 풀(설계서 2-8절, 결정 8). 이 설정이
 * 있으면 기동 때 주소마다 Publication을 미리 열고, 맵 토픽({@code matching-shard-map})이 풀에 없는
 * 주소를 가리키면 설정 어긋남으로 멈춘다. 설정이 없으면 지금처럼 Publication 1개 경로를 쓴다.
 *
 * @param endpoints 매칭 pod 주문 입구의 Aeron 채널 문자열, 최대 {@value #MAX_ENDPOINTS}개
 * @param stockSlotCount 종목 slot 개수. 매칭 pod와 같아야 한다(설계서 결정 2의 64개)
 */
@ConfigurationProperties(prefix = "matching-shard-routing")
public record MatchingShardRoutingProperties(List<String> endpoints, @DefaultValue("64") int stockSlotCount) {

    public static final int MAX_ENDPOINTS = 8;

    public MatchingShardRoutingProperties {
        endpoints = endpoints == null ? List.of() : List.copyOf(endpoints);
        if (endpoints.size() > MAX_ENDPOINTS) {
            throw new IllegalArgumentException(
                "matching-shard-routing.endpoints는 최대 " + MAX_ENDPOINTS + "개입니다: " + endpoints.size() + "개");
        }
        Set<String> distinct = new HashSet<>();
        for (String endpoint : endpoints) {
            if (endpoint == null || endpoint.isBlank()) {
                throw new IllegalArgumentException("matching-shard-routing.endpoints에 빈 주소가 있습니다");
            }
            if (!distinct.add(endpoint)) {
                throw new IllegalArgumentException("matching-shard-routing.endpoints에 같은 주소가 두 번 있습니다: " + endpoint);
            }
        }
        if (stockSlotCount < 1) {
            throw new IllegalArgumentException("matching-shard-routing.stock-slot-count는 1 이상이어야 합니다: " + stockSlotCount);
        }
    }
}
