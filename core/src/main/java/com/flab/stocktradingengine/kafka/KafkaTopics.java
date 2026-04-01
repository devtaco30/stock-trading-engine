package com.flab.stocktradingengine.kafka;

public final class KafkaTopics {

    private KafkaTopics() {}

    public static String orderRequests() { return "order-requests"; }

    public static String orders(String stockCode) { return "orders." + stockCode; }

    public static String fills(String stockCode) { return "fills." + stockCode; }
}
