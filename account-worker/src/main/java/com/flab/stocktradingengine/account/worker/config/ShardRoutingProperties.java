package com.flab.stocktradingengine.account.worker.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * I8 U2 — 계좌 워커가 자기 담당 슬롯을 판정하는 데 쓰는 정적 샤드 라우팅 설정. api·matching-worker의
 * 같은 이름 클래스와 같은 {@code shard-routing.*} 설정 키를 그대로 공유한다(계좌 슬롯 소유 표는
 * 이 값 하나뿐이어야 한다 — 담당 슬롯을 적는 별도 프로퍼티를 계좌 워커에 새로 만들지 않는다).
 * {@link com.flab.stocktradingengine.aeron.ShardRoutingTable}(순수 로직, core)은 공유하지만, 이
 * Spring 배선 클래스는 core로 옮기지 않는다 — core는 프레임워크 의존성이 없는 라이브러리로
 * 유지하기로 했다(decision_records/matching-fill-outbox.md 참고).
 */
@ConfigurationProperties(prefix = "shard-routing")
public record ShardRoutingProperties(int slotCount, List<ShardConfig> shards) {

    public ShardRoutingProperties {
        shards = shards == null ? List.of() : shards;
    }

    public record ShardConfig(String endpoint, int slotFrom, int slotTo) {
    }
}
