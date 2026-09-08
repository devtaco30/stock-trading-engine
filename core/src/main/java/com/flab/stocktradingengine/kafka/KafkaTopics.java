package com.flab.stocktradingengine.kafka;

public final class KafkaTopics {

    private KafkaTopics() {}

    public static String orderRequests() { return "order-requests"; }

    /** 단일 토픽. 종목은 파티션 키로 분배된다(같은 종목 → 같은 파티션 → 순서 보장). */
    public static String orders() { return "orders"; }

    /** 단일 토픽. 종목은 파티션 키로 분배된다. */
    public static String fills() { return "fills"; }

    /**
     * v2 계좌 워커(account-worker)용 체결 fan-out 토픽. accountId 가 파티션 키다(계좌 소유권과
     * 일치시키기 위함 — {@link #fills()} 는 stockCode 키라 계좌 샤딩과 어긋난다).
     * 체결 하나당 매수·매도 계좌 앞으로 각각 한 메시지씩 발행된다(같은 {@code TradeFilledEvent}를
     * accountId만 다르게 키잉). v1 {@code fills} 토픽과는 별개이며 v1 파이프라인에 영향을 주지 않는다.
     */
    public static String accountFills() { return "account-fills"; }
}
