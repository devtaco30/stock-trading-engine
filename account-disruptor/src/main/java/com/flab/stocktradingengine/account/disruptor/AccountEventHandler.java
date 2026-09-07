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
        long orderId = event.getOrderId();
        long accountId = event.getAccountId();
        String requestId = event.getRequestId();
        try {
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

            BigDecimal orderAmount = price.multiply(BigDecimal.valueOf(event.getQuantity()));
            ReserveResult result = state.tryReserve(orderId, orderAmount);
            if (result.accepted()) {
                listener.onAccepted(accountId, orderId, requestId, result.reservedMargin());
            } else {
                listener.onRejected(accountId, orderId, requestId, result.reason());
            }
        } finally {
            // 슬롯 재사용 대비: 마지막 소비자이므로 처리 후 비운다.
            event.clear();
        }
    }
}
