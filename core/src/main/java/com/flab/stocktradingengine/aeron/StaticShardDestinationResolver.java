package com.flab.stocktradingengine.aeron;

import java.util.Optional;

/**
 * 계좌 샤딩 U1 — {@link ShardRoutingTable}(설정 파일의 슬롯 표)에 위임하는
 * {@link AccountDestinationResolver} 구현.
 */
public final class StaticShardDestinationResolver implements AccountDestinationResolver {

    private final ShardRoutingTable shardRoutingTable;

    public StaticShardDestinationResolver(ShardRoutingTable shardRoutingTable) {
        this.shardRoutingTable = shardRoutingTable;
    }

    /**
     * {@link ShardRoutingTable}은 기동 시점에 슬롯 커버리지를 검증해(갭·중복 0) 실제로는 null도
     * 예외도 주지 않는다. 그래도 이 메서드가 인터페이스 계약(맡은 워커가 없으면 empty)을 지키게
     * 방어적으로 감싼다 — {@link ShardRoutingTable}이 나중에 바뀌어도 보내는 쪽이 NPE로 죽지
     * 않게 하는 것이 목적이다.
     */
    @Override
    public Optional<String> endpointFor(long accountId) {
        try {
            return Optional.ofNullable(shardRoutingTable.endpointFor(accountId));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }
}
