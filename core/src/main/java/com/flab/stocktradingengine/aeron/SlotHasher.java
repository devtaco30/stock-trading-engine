package com.flab.stocktradingengine.aeron;

/**
 * 계좌번호를 칸(슬롯) 번호로 바꾼다. 칸 개수 하나만 알면 되는 순수 계산이라, 목적지를 아는
 * 부품과 분리해 둔다.
 *
 * <p>분리한 이유는 {@link ShardRoutingTable}이 "칸 계산"과 "칸 → 목적지 조회"를 같이 갖고 있었기
 * 때문이다. 조정 모드(Kafka 컨슈머 그룹 배정)에서는 목적지를 설정에 적지 않으므로 그 표가 칸
 * 1개짜리로 만들어졌고, 목적지가 없다는 이유로 칸 계산까지 같이 무너져 모든 계좌가 0번 칸으로
 * 갔다. 계산은 어느 앱에서 돌든 같은 답이 나와야 하는 순수 함수이고, 목적지 조회는 배정에 따라
 * 계속 변하는 상태라 수명이 다르다.</p>
 *
 * <p><b>칸 개수는 모든 앱이 같아야 한다.</b> 하나라도 다르면 보내는 앱과 받는 워커가 서로 다른
 * 칸 번호를 계산해, 주문은 도착하지만 받은 워커가 자기 담당이 아니라며 거부한다.</p>
 */
public final class SlotHasher {

    private final int slotCount;

    public SlotHasher(int slotCount) {
        if (slotCount < 1) {
            throw new IllegalArgumentException("칸 개수는 1 이상이어야 한다: " + slotCount);
        }
        this.slotCount = slotCount;
    }

    public int slotCount() {
        return slotCount;
    }

    /**
     * accountId가 속한 칸 번호. murmur3 finalizer(fmix64)로 비트를 고르게 섞은 뒤 칸 개수로 나눈
     * 나머지를 쓴다 — accountId가 Snowflake라 하위 비트에 순차 증가분이 몰려 있어, accountId를
     * 그대로 나누면 특정 칸에 몰린다.
     */
    public int slotFor(long accountId) {
        return Math.floorMod(fmix64(accountId), slotCount);
    }

    private static long fmix64(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }
}
