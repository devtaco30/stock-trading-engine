package com.flab.stocktradingengine.account.disruptor.handler;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.math.BigDecimal;
import java.util.Map;

import com.lmax.disruptor.EventHandler;
import com.flab.stocktradingengine.account.disruptor.domain.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.domain.AccountState;
import com.flab.stocktradingengine.account.disruptor.domain.BuyFillResult;
import com.flab.stocktradingengine.account.disruptor.domain.RejectReason;
import com.flab.stocktradingengine.account.disruptor.domain.ReserveResult;
import com.flab.stocktradingengine.account.disruptor.domain.SellReserveResult;
import com.flab.stocktradingengine.account.disruptor.engine.AccountEvent;
import com.flab.stocktradingengine.account.disruptor.engine.AccountOrderIdGenerator;
import com.flab.stocktradingengine.account.disruptor.io.MatchingOrderSender;
import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * 링버퍼를 소비하는 단일 계좌 핸들러.
 *
 * <p>Disruptor 가 이 핸들러를 스레드 하나에만 배정하므로, 계좌 맵을 일반 {@link Map}(HashMap)으로
 * 둔다 — 단일 스레드만 접근해 락·동시성 맵이 필요 없다. matching-disruptor 의 {@code books} 와 같은 이유다.
 * 검증·예약 규칙은 {@link AccountState} 가 책임하고, 이 핸들러는 이벤트를 계좌에 넘기고 결과를
 * {@link AccountResultListener} 로 내보내는 얇은 껍데기다.</p>
 *
 * <h3>orderId 발급 (C5-2a, 2b-0)</h3>
 * <p>v2 핫패스엔 DB가 없어 orderId(주문 신원) 발급 위치를 계좌 워커(single-writer)로 뒀다.
 * requestId가 처음 등장하면 {@link #orderIdGenerator}(결정론적, 2b-0)로 orderId를 발급해
 * {@link AccountState}에 기억시키고, 재전송이면 기억해둔 orderId를 그대로 돌려준다 — accept·reject
 * 결과와 무관하게 발급 자체는 첫 등장에서 한 번뿐이다(리플레이 때도 카운터가 같은 횟수만 증가해
 * 같은 orderId가 나오는 이유).</p>
 *
 * <h3>매칭으로 발신 (②-b)</h3>
 * <p>매수·매도가 accept됐을 때만(onAccepted·onSellAccepted 뒤) {@link #matchingOrderSender}로
 * 매칭에 넘긴다 — 거부·중복·invalid는 매칭이 몰라도 되는 상태라 발신하지 않는다.</p>
 */
public class AccountEventHandler implements EventHandler<AccountEvent> {

    private static final Logger log = System.getLogger(AccountEventHandler.class.getName());

    /** orderId가 발급되지 못했을 때(requestId 빈값·null, 모르는 계좌) 리스너에 싣는 값 — 발급기는 0을 내지 않는다. */
    private static final long NO_ORDER_ID = 0L;

    private final Map<Long, AccountState> accounts;
    private final AccountOrderIdGenerator orderIdGenerator;
    private final MatchingOrderSender matchingOrderSender;
    private final AccountResultListener listener;

    public AccountEventHandler(Map<Long, AccountState> accounts, AccountOrderIdGenerator orderIdGenerator,
                               MatchingOrderSender matchingOrderSender, AccountResultListener listener) {
        this.accounts = accounts;
        this.orderIdGenerator = orderIdGenerator;
        this.matchingOrderSender = matchingOrderSender;
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
                case SETTLEMENT -> handleSettlement(event);
            }
        } catch (IllegalArgumentException | IllegalStateException e) {
            // 도메인 불변식 위반(예약 없는 체결 등)은 재시도해도 같으므로 이 이벤트만 폐기한다.
            // 매칭 핸들러(MatchingEventHandler)와 대칭 — 저널에 이미 박힌 poison도 recover 때
            // 같은 catch로 걸러 무한루프를 끊는다.
            log.log(Level.WARNING, "[계좌] 이벤트 폐기: type=" + event.getType()
                + " accountId=" + event.getAccountId() + " orderId=" + event.getOrderId()
                + " 이유=" + e.getMessage());
        } finally {
            // 슬롯 재사용 대비: 마지막 소비자이므로 처리 후 비운다.
            event.clear();
        }
    }

    private void handleBuy(AccountEvent event) {
        long accountId = event.getAccountId();
        String requestId = event.getRequestId();

        if (requestId == null || requestId.isBlank()) {
            // 재전송 멱등키가 없으면 requestIdToOrderId에 빈 키가 들어가 서로 다른 주문의
            // 둘째가 재전송으로 오인될 수 있다 — orderId를 발급하지 않고 검증 전에 거부한다.
            listener.onRejected(accountId, NO_ORDER_ID, requestId, RejectReason.INVALID_REQUEST_ID);
            return;
        }

        AccountState state = accounts.get(accountId);
        if (state == null) {
            // 워커가 소유하지 않은 계좌 — 라우팅이 잘못됐거나 시드 누락
            listener.onRejected(accountId, NO_ORDER_ID, requestId, RejectReason.ACCOUNT_NOT_FOUND);
            return;
        }
        Long existingOrderId = state.orderIdFor(requestId);
        if (existingOrderId != null) {
            listener.onDuplicateRequest(accountId, existingOrderId, requestId);
            return;
        }
        long orderId = orderIdGenerator.next();
        state.rememberRequest(requestId, orderId);

        BigDecimal price = event.getPrice();
        if (event.getQuantity() <= 0 || price == null || price.signum() <= 0) {
            listener.onRejected(accountId, orderId, requestId, RejectReason.INVALID_QUANTITY);
            return;
        }

        ReserveResult result = state.tryReserve(orderId, price, event.getQuantity());
        if (result.accepted()) {
            listener.onAccepted(accountId, orderId, requestId, result.reservedMargin());
            matchingOrderSender.forwardPlace(orderId, accountId, event.getStockCode(), OrderSide.BUY, price, event.getQuantity());
        } else {
            listener.onRejected(accountId, orderId, requestId, result.reason());
        }
    }

    private void handleSell(AccountEvent event) {
        long accountId = event.getAccountId();
        String requestId = event.getRequestId();

        if (requestId == null || requestId.isBlank()) {
            listener.onRejected(accountId, NO_ORDER_ID, requestId, RejectReason.INVALID_REQUEST_ID);
            return;
        }

        AccountState state = accounts.get(accountId);
        if (state == null) {
            listener.onRejected(accountId, NO_ORDER_ID, requestId, RejectReason.ACCOUNT_NOT_FOUND);
            return;
        }
        Long existingOrderId = state.orderIdFor(requestId);
        if (existingOrderId != null) {
            listener.onDuplicateRequest(accountId, existingOrderId, requestId);
            return;
        }
        long orderId = orderIdGenerator.next();
        state.rememberRequest(requestId, orderId);

        // 예약(트리거)엔 price를 안 쓰지만, 매칭 전달용 필드라 여기서도 매수와 대칭으로 검증한다(②-a).
        BigDecimal price = event.getPrice();
        if (event.getQuantity() <= 0 || price == null || price.signum() <= 0) {
            listener.onRejected(accountId, orderId, requestId, RejectReason.INVALID_QUANTITY);
            return;
        }

        SellReserveResult result = state.trySellReserve(orderId, event.getStockCode(), event.getQuantity());
        if (result.accepted()) {
            listener.onSellAccepted(accountId, orderId, requestId, result.reservedQuantity());
            matchingOrderSender.forwardPlace(orderId, accountId, event.getStockCode(), OrderSide.SELL, price, event.getQuantity());
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
        BuyFillResult result = state.applyBuyFill(tradeId, orderId, event.getStockCode(), event.getPrice(), event.getQuantity());
        if (result.applied()) {
            listener.onStateChanged(accountId, state.balance(), state.holdings(), state.seq());
        }
        listener.onFillApplied(accountId, orderId, tradeId, result.applied());
        if (result.applied() && result.unpaidThis().signum() > 0) {
            listener.onUnpaidRecorded(accountId, tradeId, result.unpaidThis());
        }
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
        if (applied) {
            listener.onStateChanged(accountId, state.balance(), state.holdings(), state.seq());
        }
        listener.onFillApplied(accountId, orderId, tradeId, applied);
    }

    private void handleSettlement(AccountEvent event) {
        long accountId = event.getAccountId();
        long settlementRef = event.getTradeId(); // settlementRef 는 tradeId 필드 재사용(AccountEvent 참고)

        AccountState state = accounts.get(accountId);
        if (state == null) {
            listener.onRejected(accountId, 0L, event.getRequestId(), RejectReason.ACCOUNT_NOT_FOUND);
            return;
        }
        boolean applied = state.applySettlement(settlementRef, event.getPrice());
        if (applied) {
            listener.onStateChanged(accountId, state.balance(), state.holdings(), state.seq());
        }
        listener.onSettlementApplied(accountId, settlementRef, applied);
    }
}
