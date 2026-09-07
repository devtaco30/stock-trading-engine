package com.flab.stocktradingengine.account.disruptor;

import java.math.BigDecimal;
import java.util.Map;

import com.lmax.disruptor.EventHandler;

/**
 * 링버퍼를 소비하는 단일 계좌 핸들러.
 *
 * <p>Disruptor 가 이 핸들러를 스레드 하나에만 배정하므로, 계좌 맵을 일반 {@link Map}(HashMap)으로
 * 둔다 — 단일 스레드만 접근해 락·동시성 맵이 필요 없다. matching-disruptor 의 {@code books} 와 같은 이유다.
 * 검증·예약 규칙은 {@link AccountState} 가 책임하고, 이 핸들러는 이벤트를 계좌에 넘기고 결과를
 * {@link AccountResultListener} 로 내보내는 얇은 껍데기다.</p>
 */
public class AccountEventHandler implements EventHandler<AccountEvent> {

    private final Map<Long, AccountState> accounts;
    private final AccountResultListener listener;

    public AccountEventHandler(Map<Long, AccountState> accounts, AccountResultListener listener) {
        this.accounts = accounts;
        this.listener = listener;
    }

    @Override
    public void onEvent(AccountEvent event, long sequence, boolean endOfBatch) {
        try {
            switch (event.getType()) {
                case BUY -> handleBuy(event);
                case SELL -> handleSell(event);
                case BUY_FILL -> handleBuyFill(event);
                case SELL_FILL -> handleSellFill(event);
            }
        } finally {
            // 슬롯 재사용 대비: 마지막 소비자이므로 처리 후 비운다.
            event.clear();
        }
    }

    private void handleBuy(AccountEvent event) {
        long orderId = event.getOrderId();
        long accountId = event.getAccountId();
        String requestId = event.getRequestId();

        AccountState state = accounts.get(accountId);
        if (state == null) {
            // 워커가 소유하지 않은 계좌 — 라우팅이 잘못됐거나 시드 누락
            listener.onRejected(accountId, orderId, requestId, RejectReason.ACCOUNT_NOT_FOUND);
            return;
        }
        BigDecimal price = event.getPrice();
        if (event.getQuantity() <= 0 || price == null || price.signum() <= 0) {
            listener.onRejected(accountId, orderId, requestId, RejectReason.INVALID_QUANTITY);
            return;
        }

        ReserveResult result = state.tryReserve(orderId, price, event.getQuantity());
        if (result.accepted()) {
            listener.onAccepted(accountId, orderId, requestId, result.reservedMargin());
        } else {
            listener.onRejected(accountId, orderId, requestId, result.reason());
        }
    }

    private void handleSell(AccountEvent event) {
        long orderId = event.getOrderId();
        long accountId = event.getAccountId();
        String requestId = event.getRequestId();

        AccountState state = accounts.get(accountId);
        if (state == null) {
            listener.onRejected(accountId, orderId, requestId, RejectReason.ACCOUNT_NOT_FOUND);
            return;
        }
        if (event.getQuantity() <= 0) {
            listener.onRejected(accountId, orderId, requestId, RejectReason.INVALID_QUANTITY);
            return;
        }

        SellReserveResult result = state.trySellReserve(orderId, event.getStockCode(), event.getQuantity());
        if (result.accepted()) {
            listener.onSellAccepted(accountId, orderId, requestId, result.reservedQuantity());
        } else {
            listener.onRejected(accountId, orderId, requestId, result.reason());
        }
    }

    private void handleBuyFill(AccountEvent event) {
        long accountId = event.getAccountId();
        long orderId = event.getOrderId();
        long tradeId = event.getTradeId();

        AccountState state = accounts.get(accountId);
        if (state == null) {
            listener.onRejected(accountId, orderId, event.getRequestId(), RejectReason.ACCOUNT_NOT_FOUND);
            return;
        }
        boolean applied = state.applyBuyFill(tradeId, orderId, event.getStockCode(), event.getPrice(), event.getQuantity());
        listener.onFillApplied(accountId, orderId, tradeId, applied);
    }

    private void handleSellFill(AccountEvent event) {
        long accountId = event.getAccountId();
        long orderId = event.getOrderId();
        long tradeId = event.getTradeId();

        AccountState state = accounts.get(accountId);
        if (state == null) {
            listener.onRejected(accountId, orderId, event.getRequestId(), RejectReason.ACCOUNT_NOT_FOUND);
            return;
        }
        boolean applied = state.applySellFill(tradeId, orderId, event.getStockCode(), event.getQuantity());
        listener.onFillApplied(accountId, orderId, tradeId, applied);
    }
}
