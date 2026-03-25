package com.flab.stocktradingengine.trading.kafka;

/**
 * Kafka 토픽 이름 규칙.
 *
 * <pre>
 * order-requests         — 주문·취소 요청 이벤트 (API 서버 → OrderRequestConsumer, key=accountId)
 * orders.{stockCode}  — 주문 접수·취소 이벤트 (OrderRequestConsumer → 매칭 컨슈머, key=stockCode)
 * fills.{stockCode}   — 체결 이벤트          (매칭 컨슈머 → Settlement 컨슈머, key=stockCode)
 * </pre>
 *
 * order-requests: key = accountId → 같은 계좌의 주문이 항상 같은 파티션 → 직렬 처리 (DB 비관적 락 제거)
 * orders.{stockCode}: key = stockCode → 매칭 컨슈머 Single Writer 원칙 보장
 */
public final class KafkaTopics {

    private KafkaTopics() {}

    public static String orderRequests() {
        return "order-requests";
    }

    public static String orders(String stockCode) {
        return "orders." + stockCode;
    }

    public static String fills(String stockCode) {
        return "fills." + stockCode;
    }
}
