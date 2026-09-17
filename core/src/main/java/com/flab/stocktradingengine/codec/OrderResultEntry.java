package com.flab.stocktradingengine.codec;

/**
 * 주문 접수 판정 한 건. account-worker가 {@code AccountResultListener} 콜백에서 만들어 Archive에
 * 기록하고, 별도 스레드가 이걸 그대로 Kafka로 옮긴다.
 *
 * <p>{@code rejectReason}은 {@code account-disruptor}의 {@code RejectReason} enum 이름을 담는다
 * (core는 account-disruptor에 의존할 수 없어 타입을 그대로 참조하지 못한다 — 의존 순서는
 * core → account-disruptor).</p>
 */
public record OrderResultEntry(
    long accountId,
    long orderId,        // 발급 전 거부면 0 (AccountEventHandler.NO_ORDER_ID와 같은 뜻)
    String requestId,
    OrderVerdict verdict,
    String rejectReason,  // REJECTED일 때만 non-null, 아니면 null
    long epochMillis
) {}
