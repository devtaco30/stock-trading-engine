package com.flab.stocktradingengine.trading.redis;

/**
 * Redis 키 네이밍 규칙.
 * MatchingConsumer(쓰기)와 api 서비스(읽기)가 같은 키를 사용하도록 단일 정의.
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
