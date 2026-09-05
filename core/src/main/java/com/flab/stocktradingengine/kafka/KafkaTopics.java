package com.flab.stocktradingengine.kafka;

public final class KafkaTopics {

    private KafkaTopics() {}

    public static String orderRequests() { return "order-requests"; }

    /** 단일 토픽. 종목은 파티션 키로 분배된다(같은 종목 → 같은 파티션 → 순서 보장). */
    public static String orders() { return "orders"; }

    /** 단일 토픽. 종목은 파티션 키로 분배된다. */
    public static String fills() { return "fills"; }
}
