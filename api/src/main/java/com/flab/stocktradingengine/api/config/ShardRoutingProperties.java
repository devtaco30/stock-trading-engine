package com.flab.stocktradingengine.api.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * I8 U1 — api가 계좌 인테이크 fan-out 목적지를 정하는 정적 샤드 라우팅 설정. matching-worker의
 * {@code ShardRoutingProperties}와 같은 {@code shard-routing.*} 설정 키를 그대로 공유한다(계좌
 * 슬롯 소유 표는 이 값 하나뿐이어야 한다). {@link com.flab.stocktradingengine.aeron.ShardRoutingTable}
 * (순수 로직, core)은 이미 공유되지만, 이 Spring 배선 클래스는 core로 옮기지 않는다 — core는
 * 프레임워크 의존성이 없는 라이브러리로 유지하기로 했다(decision_records 참고, ADR-018과 같은 결).
 * 그래서 api·matching-worker·account-worker가 각자 얇은 사본을 갖는다.
 */
@ConfigurationProperties(prefix = "shard-routing")
public record ShardRoutingProperties(int slotCount, List<ShardConfig> shards, List<String> endpoints) {

    /**
     * {@code shard-routing.shards}가 아예 안 주어지면(샤딩 미도입 환경, 기존 통합테스트들처럼
     * 이 설정을 모르는 컨텍스트) 바인더가 null을 준다. null이면 {@link ShardRoutingConfig}가
     * 바로 for-each/stream 해 기동 자체가 NPE로 죽으므로, 빈 리스트로 정규화해 "샤딩 미설정"과
     * "shards 있음"을 {@link ShardRoutingConfig}가 안전하게 구분할 수 있게 한다.
     *
     * <p>{@code endpoints}(계좌 샤딩 U5)는 {@code shards}와 다른 것을 적는 자리다 — "슬롯 몇 번부터
     * 몇 번까지 누가 담당"이 아니라 "존재하는 워커 주소가 무엇인가"(멤버 목록)만 적는다. 담당
     * 배정은 Kafka가 정하므로(U4), Publication을 미리 열어둘 풀만 여기서 안다.</p>
     */
    public ShardRoutingProperties {
        shards = shards == null ? List.of() : shards;
        endpoints = endpoints == null ? List.of() : endpoints;
    }

    public record ShardConfig(String endpoint, int slotFrom, int slotTo) {
    }
}
