package com.flab.stocktradingengine.account.disruptor;

import java.math.BigDecimal;

/**
 * 콜백을 전부 무시하는 {@link AccountResultListener}(2b-2). {@link AccountEngine#recover}가 저널을
 * 재적용할 때 쓴다 — 이미 일어난 일을 다시 바깥에 통지할 이유가 없어서다.
 */
final class NoOpAccountResultListener implements AccountResultListener {

    static final NoOpAccountResultListener INSTANCE = new NoOpAccountResultListener();

    private NoOpAccountResultListener() {
    }

    @Override
    public void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin) {
    }

    @Override
    public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
    }

    @Override
    public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
    }

    @Override
    public void onFillApplied(long accountId, long orderId, long tradeId, boolean applied) {
    }

    @Override
    public void onSettlementApplied(long accountId, long settlementRef, boolean applied) {
    }

    @Override
    public void onUnpaidRecorded(long accountId, long tradeId, BigDecimal amount) {
    }

    @Override
    public void onDuplicateRequest(long accountId, long orderId, String requestId) {
    }
}
