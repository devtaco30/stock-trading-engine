package com.flab.stocktradingengine.account.disruptor.domain;

import java.math.BigDecimal;
import java.util.Map;

/**
 * 검증·예약 결과를 계좌 워커 밖으로 내보내는 출구.
 *
 * <p>소비자 스레드가 매수 한 건을 처리할 때마다 통과/거부 중 하나를 호출한다.
 * B2 에서는 테스트가 이 인터페이스를 구현해 결과를 수집·검증한다.
 * 이후 통합 단계에서 통과분을 Aeron 으로 매칭에 전달하는 자리가 된다.</p>
 */
public interface AccountResultListener {

    /** 매수 검증 통과 — reservedMargin 은 이번 주문에 예약된 증거금. */
    void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin);

    /** 매도 검증 통과 — reservedQuantity 는 이번 주문에 예약된 수량. */
    void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity);

    /** 검증 거부 — reason 은 거부 사유. */
    void onRejected(long accountId, long orderId, String requestId, RejectReason reason);

    /**
     * 체결 반영 처리를 통지한다 — applied=true 면 이번 호출로 실제 반영, false 면 같은 tradeId
     * 재도착이라 무시(멱등)했다는 뜻.
     */
    void onFillApplied(long accountId, long orderId, long tradeId, boolean applied);

    /**
     * 정산(T+2) 되돌림 처리를 통지한다 — applied=true 면 이번 호출로 실제 반영, false 면 같은
     * settlementRef 재도착이라 무시(멱등)했다는 뜻.
     */
    void onSettlementApplied(long accountId, long settlementRef, boolean applied);

    /**
     * 매수 체결이 미수금을 남겼을 때 통지한다 — settlement-requests 발행 트리거다.
     * amount 는 이번 체결로 새로 생긴 미수금(누적 아님). tradeId 는 그 미수금의 멱등키(settlementRef)로 쓰인다.
     */
    void onUnpaidRecorded(long accountId, long tradeId, BigDecimal amount);

    /**
     * 매수·매도 접수 재전송을 통지한다 — 같은 requestId 가 이미 처리된 적이 있어(accept·reject
     * 무관) 재예약 없이 무시했다는 뜻. 클라이언트는 requestId 로 원래 결과를 폴링해서 본다.
     *
     * <p>orderId 는 안 싣는다(릭 수정 U2) — 재전송의 원래 orderId를 실제로 쓰는 소비자가 없었다
     * (로그 한 줄뿐, 클라이언트 응답 경로로도 안 나감).</p>
     */
    void onDuplicateRequest(long accountId, String requestId);

    /**
     * 잔고·보유가 실제로 바뀌었을 때 통지한다(계좌 상태 영속/프로젝션 트랙 Unit 2) —
     * applyBuyFill·applySellFill·applySettlement가 실제로 반영됐을 때만(예약 accept·거부·멱등
     * 무시는 잔고·보유가 안 바뀌므로 호출되지 않는다). 캡처는 반드시 호출 시점(엔진 스레드)의
     * 값이어야 한다 — 호출부가 그 시점의 balance·holdings·seq를 그대로 넘긴다.
     *
     * <p>기본 구현은 no-op이다 — 이 통지가 필요 없는 기존 리스너(로깅 등)는 그대로 둘 수 있다.</p>
     */
    default void onStateChanged(long accountId, BigDecimal balance, Map<String, Integer> holdings, long seq) {
    }
}
