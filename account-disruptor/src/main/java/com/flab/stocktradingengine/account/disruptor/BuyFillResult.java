package com.flab.stocktradingengine.account.disruptor;

import java.math.BigDecimal;

/**
 * 매수 체결 반영 결과(불변 값).
 * <p>applied 면 unpaidThis 에 이번 체결로 새로 생긴 미수금(누적이 아니라 이번 체결분만)이 담긴다.
 * 같은 tradeId 재도착이라 무시했으면 applied=false, unpaidThis=0.</p>
 */
public record BuyFillResult(boolean applied, BigDecimal unpaidThis) {

    public static BuyFillResult notApplied() {
        return new BuyFillResult(false, BigDecimal.ZERO);
    }
}
