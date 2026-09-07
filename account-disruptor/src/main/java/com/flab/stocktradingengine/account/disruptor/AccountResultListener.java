package com.flab.stocktradingengine.account.disruptor;

import java.math.BigDecimal;

/**
 * 검증·예약 결과를 계좌 워커 밖으로 내보내는 출구.
 *
 * <p>소비자 스레드가 매수 한 건을 처리할 때마다 통과/거부 중 하나를 호출한다.
 * B2 에서는 테스트가 이 인터페이스를 구현해 결과를 수집·검증한다.
 * 이후 통합 단계에서 통과분을 Aeron 으로 매칭에 전달하는 자리가 된다.</p>
 */
public interface AccountResultListener {

    /** 검증 통과 — reservedMargin 은 이번 주문에 예약된 증거금. */
    void onAccepted(long accountId, String requestId, BigDecimal reservedMargin);

    /** 검증 거부 — reason 은 거부 사유. */
    void onRejected(long accountId, String requestId, RejectReason reason);
}
