package com.flab.stocktradingengine.account.worker;

import java.math.BigDecimal;
import java.util.List;

import com.flab.stocktradingengine.account.disruptor.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.RejectReason;

/**
 * {@link AccountResultListener} 구현체 여러 개를 하나로 묶어 콜백마다 전부에게 전달한다.
 * {@code AccountEngine}은 리스너를 하나만 받는데(AccountEngineConfig), account-worker는
 * 로깅({@link LoggingAccountResultListener})과 정산 요청 발행({@link SettlementRequestPublisher})을
 * 동시에 등록해야 해서 필요하다.
 */
public class CompositeAccountResultListener implements AccountResultListener {

    private final List<AccountResultListener> delegates;

    public CompositeAccountResultListener(List<AccountResultListener> delegates) {
        this.delegates = delegates;
    }

    @Override
    public void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin) {
        delegates.forEach(delegate -> delegate.onAccepted(accountId, orderId, requestId, reservedMargin));
    }

    @Override
    public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
        delegates.forEach(delegate -> delegate.onSellAccepted(accountId, orderId, requestId, reservedQuantity));
    }

    @Override
    public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
        delegates.forEach(delegate -> delegate.onRejected(accountId, orderId, requestId, reason));
    }

    @Override
    public void onFillApplied(long accountId, long orderId, long tradeId, boolean applied) {
        delegates.forEach(delegate -> delegate.onFillApplied(accountId, orderId, tradeId, applied));
    }

    @Override
    public void onSettlementApplied(long accountId, long settlementRef, boolean applied) {
        delegates.forEach(delegate -> delegate.onSettlementApplied(accountId, settlementRef, applied));
    }

    @Override
    public void onUnpaidRecorded(long accountId, long tradeId, BigDecimal amount) {
        delegates.forEach(delegate -> delegate.onUnpaidRecorded(accountId, tradeId, amount));
    }
}
