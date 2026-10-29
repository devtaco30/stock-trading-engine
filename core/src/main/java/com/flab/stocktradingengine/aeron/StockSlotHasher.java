package com.flab.stocktradingengine.aeron;

/**
 * 종목코드를 종목 slot 번호로 바꾼다. 계좌번호용 {@link SlotHasher}와 같은 fmix64 섞기를 쓰되,
 * 입력이 문자열이라 {@link String#hashCode()}(Java 언어 명세가 계산식을 정해 둔 32비트 값이라
 * 어느 JVM에서나 같다)를 64비트로 넓혀 넘긴다.
 *
 * <p>종목 slot 개수는 매칭 pod와 계좌 shard를 포함해 모든 앱이 같아야 한다. 다르면 보내는 쪽과
 * 받는 쪽이 서로 다른 번호를 계산한다.</p>
 */
public final class StockSlotHasher {

    private final SlotHasher slotHasher;

    public StockSlotHasher(int stockSlotCount) {
        if (stockSlotCount < 1) {
            throw new IllegalArgumentException("종목 slot 개수는 1 이상이어야 한다: " + stockSlotCount);
        }
        this.slotHasher = new SlotHasher(stockSlotCount);
    }

    public int stockSlotCount() {
        return slotHasher.slotCount();
    }

    public int slotFor(String stockCode) {
        long widenedHash = stockCode.hashCode();
        return slotHasher.slotFor(widenedHash);
    }
}
