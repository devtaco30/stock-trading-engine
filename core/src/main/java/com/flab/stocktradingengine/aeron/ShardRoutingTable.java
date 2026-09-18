package com.flab.stocktradingengine.aeron;

import java.util.Arrays;
import java.util.List;

/**
 * fork3, Unit 1 — accountId를 정적 샤드 endpoint로 결정론적으로 매핑하는 라우팅 테이블(ADR-031).
 *
 * <p>매칭(sender)이 체결을 어느 계좌 샤드로 fan-out할지 정할 때 쓴다. 벽시계·랜덤을 쓰지 않고
 * accountId만으로 계산하는 순수 함수라 재기동·리플레이에서도 같은 결과를 낸다(결정론).</p>
 *
 * <p>슬롯 소유는 단일 writer 정합의 전제라, 생성 시점에 슬롯 0..slotCount-1이 정확히 하나의
 * endpoint에만 매핑되는지(갭·중복 0) 검증하고 위반하면 기동을 막는다(fail-fast).</p>
 */
public final class ShardRoutingTable {

    private final String[] slotToEndpoint;
    private final SlotHasher slotHasher;

    public ShardRoutingTable(int slotCount, List<ShardRange> shards) {
        this.slotToEndpoint = buildSlotToEndpoint(slotCount, shards);
        this.slotHasher = new SlotHasher(slotCount);
    }

    /**
     * accountId가 속한 슬롯 번호. murmur3 finalizer(fmix64)로 비트를 고르게 섞은 뒤 slotCount로
     * 나눈 나머지를 쓴다 — accountId가 Snowflake라 하위 비트에 순차 증가분이 몰려 있어, 그냥
     * hashCode나 accountId 자체를 나누면 특정 슬롯에 쏠릴 수 있다.
     */
    public int slotFor(long accountId) {
        return slotHasher.slotFor(accountId);
    }

    /**
     * accountId가 속한 슬롯이 맡은 Aeron endpoint 채널 문자열(fork1 udp endpoint 형식).
     */
    public String endpointFor(long accountId) {
        return slotToEndpoint[slotFor(accountId)];
    }

    /**
     * fork3, Unit 2 — 이 테이블이 가리키는 서로 다른 목적지 endpoint 전부(중복 제거). 발신측이
     * "가능한 모든 목적지마다 Publication 1개"를 빠짐없이 만드는 데 쓴다.
     */
    public List<String> distinctEndpoints() {
        return Arrays.stream(slotToEndpoint).distinct().toList();
    }

    private static String[] buildSlotToEndpoint(int slotCount, List<ShardRange> shards) {
        if (slotCount < 1) {
            throw new IllegalStateException("slot-count는 1 이상이어야 한다: " + slotCount);
        }
        if (shards == null || shards.isEmpty()) {
            throw new IllegalStateException("shard-routing.shards가 비어 있다");
        }

        String[] slotToEndpoint = new String[slotCount];
        for (ShardRange shard : shards) {
            validateRange(shard, slotCount);
            for (int slot = shard.slotFrom(); slot <= shard.slotTo(); slot++) {
                if (slotToEndpoint[slot] != null) {
                    throw new IllegalStateException(
                        "슬롯 " + slot + "이 두 endpoint에 중복 매핑됨: " + slotToEndpoint[slot] + ", " + shard.endpoint());
                }
                slotToEndpoint[slot] = shard.endpoint();
            }
        }

        for (int slot = 0; slot < slotCount; slot++) {
            if (slotToEndpoint[slot] == null) {
                throw new IllegalStateException("슬롯 " + slot + "이 어떤 endpoint에도 매핑되지 않았다(갭)");
            }
        }
        return slotToEndpoint;
    }

    private static void validateRange(ShardRange shard, int slotCount) {
        if (shard.slotFrom() < 0 || shard.slotTo() >= slotCount || shard.slotFrom() > shard.slotTo()) {
            throw new IllegalStateException(
                "잘못된 슬롯 범위: endpoint=" + shard.endpoint() + " slotFrom=" + shard.slotFrom()
                    + " slotTo=" + shard.slotTo() + " slotCount=" + slotCount);
        }
    }


    public record ShardRange(String endpoint, int slotFrom, int slotTo) {
    }
}
