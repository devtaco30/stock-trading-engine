package com.flab.stocktradingengine.kafka;

public final class KafkaTopics {

    private KafkaTopics() {}

    public static String orderRequests() { return "order-requests"; }

    /** 단일 토픽. 종목은 파티션 키로 분배된다(같은 종목 → 같은 파티션 → 순서 보장). */
    public static String orders() { return "orders"; }

    /** 단일 토픽. 종목은 파티션 키로 분배된다. */
    public static String fills() { return "fills"; }

    /**
     * v2 정산 되돌림 토픽. settlement(T+2)가 account-worker에 "실제 잔고를 차감하라"고
     * 보내는 명령 경로다(v1엔 없던 경로 — v1은 DB 직접 차감이었다). accountId 가 파티션 키다.
     */
    public static String accountSettlements() { return "account-settlements"; }

    /**
     * v2 미수금 발생 통지 토픽. account-worker가 매수 체결로 새로 생긴 미수금을 settlement에
     * "이 계좌에 이만큼, 언제까지 정산하라"고 보내는 경로다(v1엔 없던 경로 — v1은 T+2 스케줄러 없이
     * 수동 repay API였다). accountId 가 파티션 키다.
     */
    public static String settlementRequests() { return "settlement-requests"; }

    /**
     * 계좌 상태 영속/프로젝션 트랙(off-path) full-state 토픽. account-worker가 잔고·보유가
     * 실제로 바뀔 때마다 발행하고, account-projection-worker가 소비해 DB read model에
     * upsert한다(Unit 2~3). accountId가 파티션 키다(같은 계좌 순서 보장).
     */
    public static String accountState() { return "account-state"; }

    /**
     * 주문 결과 기록 트랙 토픽. account-worker가 Archive에 남긴 접수 판정(수락·거부·중복)을
     * 별도 스레드가 여기로 옮긴다. accountId가 파티션 키다(같은 계좌 순서 보장).
     */
    public static String orderResults() { return "order-results"; }
}
