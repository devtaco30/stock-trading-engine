package com.flab.stocktradingengine.kafka.event;

import com.flab.stocktradingengine.codec.OrderVerdict;

/**
 * account-worker가 주문 결과 기록 트랙에서 {@code order-results} 토픽으로 발행하는 판정 이벤트.
 * Archive에 기록된 {@code OrderResultEntry}(바이트 코덱 타입)를 그대로 보내지 않고 Jackson으로
 * JSON 직렬화하는 이 record로 옮겨 보낸다({@code AccountStateEvent}와 같은 자리).
 */
public record OrderResultEvent(
    long accountId,
    long orderId,
    String requestId,
    OrderVerdict verdict,
    String rejectReason,
    long epochMillis
) {}
