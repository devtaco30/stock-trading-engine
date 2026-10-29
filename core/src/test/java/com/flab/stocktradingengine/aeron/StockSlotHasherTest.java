package com.flab.stocktradingengine.aeron;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class StockSlotHasherTest {

    private static final int STOCK_SLOT_COUNT = 64;

    @Test
    @DisplayName("같은 종목코드는 몇 번을 불러도 같은 종목 slot이다")
    void 같은_종목코드는_같은_종목_slot이다() {
        StockSlotHasher hasher = new StockSlotHasher(STOCK_SLOT_COUNT);

        int first = hasher.slotFor("005930");
        for (int call = 0; call < 100; call++) {
            assertEquals(first, hasher.slotFor("005930"));
        }
    }

    @Test
    @DisplayName("종목 slot은 언제나 0 이상 종목 slot 개수 미만이다")
    void 종목_slot은_범위_안이다() {
        StockSlotHasher hasher = new StockSlotHasher(STOCK_SLOT_COUNT);

        for (int code = 1; code <= 3000; code++) {
            String stockCode = String.format("%06d", code);
            int slot = hasher.slotFor(stockCode);
            assertTrue(slot >= 0 && slot < STOCK_SLOT_COUNT, "범위를 벗어났다: stockCode=" + stockCode + " slot=" + slot);
        }
    }

    @Test
    @DisplayName("종목 slot은 String.hashCode()를 64비트로 넓혀 fmix64를 통과시킨 값을 종목 slot 개수로 나눈 나머지다")
    void 종목_slot은_정해진_식과_같다() {
        StockSlotHasher hasher = new StockSlotHasher(STOCK_SLOT_COUNT);
        String[] stockCodes = {"005930", "000660", "035420", "A005930"};

        for (String stockCode : stockCodes) {
            long widenedHash = stockCode.hashCode();
            int expected = Math.floorMod(fmix64(widenedHash), STOCK_SLOT_COUNT);
            assertEquals(expected, hasher.slotFor(stockCode), "stockCode=" + stockCode);
        }
    }

    @Test
    @DisplayName("종목 slot 개수가 1 미만이면 예외다")
    void 종목_slot_개수가_1_미만이면_예외다() {
        assertThrows(IllegalArgumentException.class, () -> new StockSlotHasher(0));
        assertThrows(IllegalArgumentException.class, () -> new StockSlotHasher(-1));
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
