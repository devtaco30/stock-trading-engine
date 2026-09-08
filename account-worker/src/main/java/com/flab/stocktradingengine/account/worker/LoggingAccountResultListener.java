package com.flab.stocktradingengine.account.worker;

import java.math.BigDecimal;

import org.springframework.stereotype.Component;

import com.flab.stocktradingengine.account.disruptor.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.RejectReason;

import lombok.extern.slf4j.Slf4j;

/**
 * 지금 단계의 {@link AccountResultListener} 구현 — 로그만 남긴다.
 *
 * <p>매수·매도 검증 통과분을 Aeron 으로 매칭에 전달하는 건 B4 범위 밖이다(주문 접수 경로는
 * 별도 작업). 여기서는 account-fills 소비 결과(체결 반영)를 확인할 수 있으면 충분하다.</p>
 */
@Slf4j
@Component
public class LoggingAccountResultListener implements AccountResultListener {

    @Override
    public void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin) {
        log.info("[계좌 워커] 매수 검증 통과: accountId={} orderId={} 예약증거금={}", accountId, orderId, reservedMargin);
    }

    @Override
    public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
        log.info("[계좌 워커] 매도 검증 통과: accountId={} orderId={} 예약수량={}", accountId, orderId, reservedQuantity);
    }

    @Override
    public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
        log.warn("[계좌 워커] 거부: accountId={} orderId={} 사유={}", accountId, orderId, reason);
    }

    @Override
    public void onFillApplied(long accountId, long orderId, long tradeId, boolean applied) {
        log.info("[계좌 워커] 체결 반영: accountId={} orderId={} tradeId={} applied={}", accountId, orderId, tradeId, applied);
    }

    @Override
    public void onSettlementApplied(long accountId, long settlementRef, boolean applied) {
        log.info("[계좌 워커] 정산 반영: accountId={} settlementRef={} applied={}", accountId, settlementRef, applied);
    }
}
