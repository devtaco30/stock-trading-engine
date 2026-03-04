package com.flab.stocktradingengine.trading.kafka;

/**
 * Kafka 토픽 이름 규칙.
 *
 * <pre>
 * orders.{stockCode}  — 주문 접수·취소 이벤트 (API 서버 → 매칭 컨슈머)
 * fills.{stockCode}   — 체결 이벤트          (매칭 컨슈머 → Settlement 컨슈머)
 * </pre>
 *
 * key = stockCode 로 발행해 같은 종목의 메시지가 항상 같은 파티션으로 라우팅되도록 한다.
 * 이를 통해 매칭 컨슈머의 Single Writer 원칙(종목당 1개 컨슈머)이 인프라 레벨에서 보장된다.
 */
public final class KafkaTopics {

    private KafkaTopics() {}

    public static String orders(String stockCode) {
        return "orders." + stockCode;
    }

    public static String fills(String stockCode) {
        return "fills." + stockCode;
    }
}
