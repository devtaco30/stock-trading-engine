package com.flab.stocktradingengine.account.disruptor.handler;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.math.BigDecimal;
import java.util.HashMap;
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
import com.flab.stocktradingengine.account.disruptor.io.AccountSnapshotSink;
import com.flab.stocktradingengine.account.disruptor.io.MatchingOrderSender;
import com.flab.stocktradingengine.account.disruptor.snapshot.AccountSnapshot;
import com.flab.stocktradingengine.account.disruptor.snapshot.AccountSnapshotCodec;
import com.flab.stocktradingengine.account.disruptor.snapshot.AccountStateSnapshot;
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
 * requestId가 처음 등장하면(=재전송이 아니면, {@link AccountState#isDuplicateRequest}) {@link
 * #orderIdGenerator}(결정론적, 2b-0)로 orderId를 발급한다 — 재전송이면 발급 없이 {@code
 * onDuplicateRequest}로만 알린다(릭 수정 U2 — 재전송의 원래 orderId를 돌려주는 조회는 실제로 쓰는
 * 소비자가 없어 뺐다). 발급 자체는 첫 등장에서 한 번뿐이다(리플레이 때도 카운터가 같은 횟수만
 * 증가해 같은 orderId가 나오는 이유).</p>
 *
 * <h3>매칭으로 발신 (②-b)</h3>
 * <p>매수·매도가 accept됐을 때만(onAccepted·onSellAccepted 뒤) {@link #matchingOrderSender}로
 * 매칭에 넘긴다 — 거부·중복·invalid는 매칭이 몰라도 되는 상태라 발신하지 않는다.</p>
 */
public class AccountEventHandler implements EventHandler<AccountEvent> {

    private static final Logger log = System.getLogger(AccountEventHandler.class.getName());

    /** orderId가 발급되지 못했을 때(requestId 빈값·null, 모르는 계좌) 리스너에 싣는 값 — 발급기는 0을 내지 않는다. */
    private static final long NO_ORDER_ID = 0L;

    // 저널 N건마다 러닝 중 스냅샷을 찍는다(1-3). 근거 없는 임시값이다 — account-disruptor에
    // 자체 처리량 벤치마크가 아직 없다(매칭 코어 JMH 실측만 있음, decision_records/
    // account-idempotency-cache-bound.md). C7 부하측정 이후 실측 처리량 × 감당할 복구시간으로
    // 재산정한다(settlement 500=근거 있음·requestId 5000=임시와 같은 처리).
    private static final long SNAPSHOT_INTERVAL_JOURNAL_ENTRIES = 10_000L;
    // tradeId 세대 상한 — 현재 세대 + 직전 세대(재전송 창 꼬리)만 남긴다(1-4 설계).
    private static final int KEEP_GENERATIONS = 2;

    private final Map<Long, AccountState> accounts;
    private final AccountOrderIdGenerator orderIdGenerator;
    private final MatchingOrderSender matchingOrderSender;
    private final AccountResultListener listener;
    private final AccountSnapshotSink snapshotSink;
    private final AccountSnapshotCodec snapshotCodec = new AccountSnapshotCodec();
    private final boolean snapshotTriggerEnabled;

    // 소비자 스레드(이 핸들러)만 쓰고, 호스트 스레드(그레이스풀 스톱)·1-3의 스냅샷 쓰기 스레드가
    // 읽는다 — 다른 스레드가 읽으므로 volatile.
    private volatile long lastAppliedFillPosition;
    private volatile long lastJournaledPosition;
    // 저널 적용 순번(1-3) — 이 핸들러(소비자 스레드)만 읽고 쓴다. 처리 결과(성공·거부·중복·예외)와
    // 무관하게 이벤트 하나를 소비할 때마다 1씩 증가한다 — "저널을 얼마나 소비했나"를 뜻하지
    // "상태가 몇 번 바뀌었나"(그건 AccountState.seq)를 뜻하지 않는다.
    private long appliedSeq;

    public AccountEventHandler(Map<Long, AccountState> accounts, AccountOrderIdGenerator orderIdGenerator,
                               MatchingOrderSender matchingOrderSender, AccountResultListener listener,
                               AccountSnapshotSink snapshotSink) {
        this(accounts, orderIdGenerator, matchingOrderSender, listener, snapshotSink, true);
    }

    /**
     * @param snapshotTriggerEnabled N건마다 세대 경계를 열고 스냅샷을 직렬화+offer할지. {@link
     *     com.flab.stocktradingengine.account.disruptor.engine.AccountEngine#recover}의 replay
     *     전용 핸들러만 false를 준다 — 이미 지나간 저널을 다시 훑는 것뿐이라 새로 뜰 스냅샷이
     *     없고, 그 핸들러의 싱크는 항상 durableSeq 0(NO_OP)이라 세대를 나눠도 가지치기가 아예
     *     안 일어나 결과가 켜둔 것과 같다 — 다만 매 replay마다 전체 계좌 상태를 인코딩해 버리는
     *     낭비(큰 저널일수록 복구 시간에 직접 얹힘)만 남으므로 끈다.
     */
    public AccountEventHandler(Map<Long, AccountState> accounts, AccountOrderIdGenerator orderIdGenerator,
                               MatchingOrderSender matchingOrderSender, AccountResultListener listener,
                               AccountSnapshotSink snapshotSink, boolean snapshotTriggerEnabled) {
        this.accounts = accounts;
        this.orderIdGenerator = orderIdGenerator;
        this.matchingOrderSender = matchingOrderSender;
        this.listener = listener;
        this.snapshotSink = snapshotSink;
        this.snapshotTriggerEnabled = snapshotTriggerEnabled;
    }

    /** 소비자가 실제로 처리한 시점의 체결 수신 위치(1-2) — {@code AccountFillReceiver.consumedPosition()}과 달리 아직 처리 안 된 체결은 반영하지 않는다. */
    public long lastAppliedFillPosition() {
        return lastAppliedFillPosition;
    }

    /** 소비자가 실제로 처리한 시점의 저널 위치(1-2). */
    public long lastJournaledPosition() {
        return lastJournaledPosition;
    }

    /**
     * 복구 직후(start() 전, 단일 스레드) 체결 수신 위치를 스냅샷이 가리키던 값으로 시드한다(1-3).
     * 안 하면 복구~첫 라이브 체결 사이에 러닝 중 스냅샷이 찍힐 때 fillPosition이 0으로 저장돼,
     * 다음 복구가 체결 스트림을 처음부터 다시 replay한다 — 그사이 이미 가지치기된 tradeId가 있으면
     * 이중 반영(돈)으로 이어질 수 있다.
     */
    public void seedLastAppliedFillPosition(long fillPosition) {
        this.lastAppliedFillPosition = fillPosition;
    }

    @Override
    public void onEvent(AccountEvent event, long sequence, boolean endOfBatch) {
        lastJournaledPosition = event.getJournaledPosition();
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
        appliedSeq++;
        if (snapshotTriggerEnabled && appliedSeq % SNAPSHOT_INTERVAL_JOURNAL_ENTRIES == 0) {
            takeSnapshotAndStartNewGeneration();
        }
        pruneIfDurable();
    }

    /**
     * 세대 경계를 넘기고(1-4 {@code startNewGeneration}), 현재 상태를 직렬화해 쓰기 큐에 넣는다.
     * 직렬화(메모리 복사)까지만 이 스레드가 하고, 실제 디스크 쓰기는 {@link #snapshotSink}
     * 구현체의 별도 스레드가 한다(ADR-024 "single-writer는 I/O 안 함").
     */
    private void takeSnapshotAndStartNewGeneration() {
        for (AccountState state : accounts.values()) {
            state.startNewGeneration(appliedSeq);
        }
        Map<Long, AccountStateSnapshot> accountsById = new HashMap<>();
        accounts.forEach((accountId, state) -> accountsById.put(accountId, state.toSnapshot()));
        AccountSnapshot snapshot = new AccountSnapshot(accountsById, orderIdGenerator.counter(), lastJournaledPosition);
        byte[] snapshotBytes = snapshotCodec.encode(snapshot);
        boolean offered = snapshotSink.offer(snapshotBytes, lastAppliedFillPosition, appliedSeq);
        if (!offered) {
            log.log(Level.WARNING, "[계좌] 스냅샷 쓰기 큐가 가득 차 이번 회차 스킵: appliedSeq=" + appliedSeq);
        }
    }

    /**
     * durable하다고 보고된 스냅샷이 하나라도 있으면(durableSeq&gt;0) 두 세대 전을 가지치기한다.
     * durableSeq 값 자체과 비교해 "새로 진행된 경계만" 가지치기하지 않는다 — 세대가 여러 번
     * 열린 뒤에야 durableSeq가 갱신될 수 있어(쓰기 스레드가 느림), 그렇게 하면 이미 안전한데도
     * 미뤄지는 세대가 생긴다. {@link AccountState#pruneOlderThan}은 세대 수가 keep 이하면 즉시
     * 반환하므로(1-4), 매 이벤트마다 불러도 비용이 무해하다.
     */
    private void pruneIfDurable() {
        if (snapshotSink.durableSeq() > 0) {
            for (AccountState state : accounts.values()) {
                state.pruneOlderThan(KEEP_GENERATIONS);
            }
        }
    }

    private void handleBuy(AccountEvent event) {
        long accountId = event.getAccountId();
        String requestId = event.getRequestId();

        if (requestId == null || requestId.isBlank()) {
            // 재전송 멱등키가 없으면 processedRequestIds에 빈 키가 들어가 서로 다른 주문의
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
        if (state.isDuplicateRequest(requestId)) {
            listener.onDuplicateRequest(accountId, requestId);
            return;
        }
        long orderId = orderIdGenerator.next();

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
        if (state.isDuplicateRequest(requestId)) {
            listener.onDuplicateRequest(accountId, requestId);
            return;
        }
        long orderId = orderIdGenerator.next();

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
        lastAppliedFillPosition = event.getSourcePosition();

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
        lastAppliedFillPosition = event.getSourcePosition();

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
