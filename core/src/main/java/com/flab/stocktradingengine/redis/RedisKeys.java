package com.flab.stocktradingengine.redis;

/**
 * Redis 키 네이밍 규칙.
 * 모든 모듈이 같은 키를 참조하도록 core에서 단일 정의한다.
 */
public final class RedisKeys {

    private RedisKeys() {
    }

    /** 최근 체결가(LTP). 값: BigDecimal.toPlainString() */
    public static String ltp(String stockCode) {
        return "ltp:" + stockCode;
    }

    /** 호가창 스냅샷. 값: JSON {"bids":[{"price":"...","quantity":N},...], "asks":[...]} */
    public static String orderbook(String stockCode) {
        return "orderbook:" + stockCode;
    }
}
