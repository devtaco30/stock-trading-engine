package com.flab.stocktradingengine.aeron;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class StockSlotDistributionTest {

    private static final int STOCK_SLOT_COUNT = 64;
    private static final int STOCK_COUNT = 3000;
    private static final double MAX_RATIO_TO_AVERAGE = 2.5;

    @Test
    @DisplayName("종목코드 000001~003000을 종목 slot 64개에 넣으면 가장 많이 든 종목 slot이 평균의 2.5배 이하다")
    void 종목코드가_종목_slot에_고르게_나뉜다() {
        StockSlotHasher hasher = new StockSlotHasher(STOCK_SLOT_COUNT);
        int[] countsPerSlot = new int[STOCK_SLOT_COUNT];

        for (int code = 1; code <= STOCK_COUNT; code++) {
            String stockCode = String.format("%06d", code);
            countsPerSlot[hasher.slotFor(stockCode)]++;
        }

        int maxCount = Arrays.stream(countsPerSlot).max().getAsInt();
        int minCount = Arrays.stream(countsPerSlot).min().getAsInt();
        double average = (double) STOCK_COUNT / STOCK_SLOT_COUNT;
        System.out.printf("종목 slot 분포: 최대=%d 최소=%d 평균=%.2f 최대/평균=%.2f%n",
                maxCount, minCount, average, maxCount / average);

        assertTrue(maxCount <= average * MAX_RATIO_TO_AVERAGE,
                "가장 많이 든 종목 slot이 평균의 " + MAX_RATIO_TO_AVERAGE + "배를 넘었다: 최대=" + maxCount + " 평균=" + average);
    }
}
