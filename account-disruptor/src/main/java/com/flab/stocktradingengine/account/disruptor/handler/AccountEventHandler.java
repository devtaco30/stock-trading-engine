package com.flab.stocktradingengine.account.disruptor.handler;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.lmax.disruptor.EventHandler;
import com.flab.stocktradingengine.aeron.ShardRoutingTable;
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
import com.flab.stocktradingengine.time.LatencyHistogram;
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

    // 샤딩을 안 쓰는 생성자(대부분의 테스트, I8 이전 코드)를 위한 기본값 — 슬롯 1개짜리 표 하나에
    // 모든 accountId가 매핑되고, 이 워커가 그 유일한 슬롯(0)을 담당한다고 둬서 "모든 계좌를 내가
    // 담당한다"는 예전 동작(암묵적 전제)을 그대로 재현한다.
    private static final ShardRoutingTable DEFAULT_SHARD_ROUTING_TABLE =
        new ShardRoutingTable(1, List.of(new ShardRoutingTable.ShardRange("*", 0, 0)));
    private static final Set<Integer> DEFAULT_OWNED_SLOTS = Set.of(0);

    // 접수 지연 측정(끝점①)을 안 쓰는 생성자(대부분의 테스트)를 위한 기본값 — 꺼진 채로 두면
    // record가 분기 하나만 타고 즉시 반환한다.
    private static final LatencyHistogram NO_OP_LATENCY_HISTOGRAM = new LatencyHistogram(false);

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
    private final LatencyHistogram latencyHistogram;
    private final ShardRoutingTable shardRoutingTable;
    private final Set<Integer> ownedSlots;

    // sessionId(Aeron 발행자 구분키, ADR-032 I1 D1) → 그 발행자로부터 마지막으로 반영한 체결 수신
    // 위치. 소비자 스레드(이 핸들러)만 쓰고, 호스트 스레드(그레이스풀 스톱)·1-3의 스냅샷 쓰기
    // 스레드가 읽는다 — 다른 스레드가 읽으므로 ConcurrentHashMap(단일 필드 volatile로는 맵
    // 내부 갱신의 가시성을 보장 못 한다).
    private final Map<Integer, Long> lastAppliedFillPositions = new ConcurrentHashMap<>();
    private volatile long lastJournaledPosition;
    // 저널 적용 순번(1-3) — 이 핸들러(소비자 스레드)만 읽고 쓴다. 처리 결과(성공·거부·중복·예외)와
    // 무관하게 이벤트 하나를 소비할 때마다 1씩 증가한다 — "저널을 얼마나 소비했나"를 뜻하지
    // "상태가 몇 번 바뀌었나"(그건 AccountState.seq)를 뜻하지 않는다.
    private long appliedSeq;

    /**
     * ⚠️ 계좌 샤딩 없이(모든 계좌를 담당) 만든다 — 테스트 전용. 실제 워커 배선은 {@code
     * AccountEngine}을 통해 shardRoutingTable·ownedSlots를 받는 생성자로 가야 한다(직접
     * 이 생성자를 프로덕션 배선에 쓰면 조용히 샤딩이 안 걸린다).
     */
    public AccountEventHandler(Map<Long, AccountState> accounts, AccountOrderIdGenerator orderIdGenerator,
                               MatchingOrderSender matchingOrderSender, AccountResultListener listener,
                               AccountSnapshotSink snapshotSink) {
        this(accounts, orderIdGenerator, matchingOrderSender, listener, snapshotSink, true, NO_OP_LATENCY_HISTOGRAM,
            DEFAULT_SHARD_ROUTING_TABLE, DEFAULT_OWNED_SLOTS);
    }

    /**
     * ⚠️ 위와 같은 이유로 테스트 전용(모든 계좌를 담당).
     *
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
        this(accounts, orderIdGenerator, matchingOrderSender, listener, snapshotSink, snapshotTriggerEnabled, NO_OP_LATENCY_HISTOGRAM,
            DEFAULT_SHARD_ROUTING_TABLE, DEFAULT_OWNED_SLOTS);
    }

    /**
     * @param latencyHistogram 끝점①(접수·예약) 지연 측정기(decision_records/v1-v2-e2e-measurement.md).
     *     매수·매도가 accept됐을 때만 기록한다(거부·중복은 모집단에서 뺀다 — v1의 대응 지점이
     *     같은 이유로 거부 시 그 지점에 도달하지 않는 것과 모집단을 맞춘다).
     * @param shardRoutingTable accountId가 속한 슬롯 번호를 계산하는 표(I8 U2). api가 체결
     *     fan-out에 쓰는 것과 같은 종류의 표를 계좌 워커도 그대로 읽는다 — 담당 슬롯을 적는
     *     별도 프로퍼티를 새로 만들지 않는다(같은 사실이 두 곳에 각자 적히면 어긋날 수 있다).
     * @param ownedSlots 이 워커가 담당하는 슬롯 번호 집합(계좌 샤딩 U3). {@code
     *     shardRoutingTable.slotFor(accountId)}가 이 집합에 있으면 이 워커의 담당이다.
     */
    public AccountEventHandler(Map<Long, AccountState> accounts, AccountOrderIdGenerator orderIdGenerator,
                               MatchingOrderSender matchingOrderSender, AccountResultListener listener,
                               AccountSnapshotSink snapshotSink, boolean snapshotTriggerEnabled, LatencyHistogram latencyHistogram,
                               ShardRoutingTable shardRoutingTable, Set<Integer> ownedSlots) {
        this.accounts = accounts;
        this.orderIdGenerator = orderIdGenerator;
        this.matchingOrderSender = matchingOrderSender;
        this.listener = listener;
        this.snapshotSink = snapshotSink;
        this.snapshotTriggerEnabled = snapshotTriggerEnabled;
        this.latencyHistogram = latencyHistogram;
        this.shardRoutingTable = shardRoutingTable;
        this.ownedSlots = ownedSlots;
    }

    /** 이 워커가 accountId가 속한 슬롯을 담당하는지 — "내 담당인가"를 묻는 자리를 이 메서드 하나로 모은다(D2). */
    private boolean isOwned(long accountId) {
        return ownedSlots.contains(shardRoutingTable.slotFor(accountId));
    }

    /**
     * 담당이 아니면 {@link RejectReason#NOT_OWNED}로 거부하고 {@code true}를 돌려준다(호출부가
     * 그 자리에서 return하게). 계좌가 메모리에 없는 것({@link RejectReason#ACCOUNT_NOT_FOUND})과는
     * 다른 사실이라 사유를 분리한다 — 라우팅이 잘못 왔을 때 둘이 같은 사유로 보이면 원인을 못
     * 찾는다(D3).
     */
    private boolean rejectIfNotOwned(long accountId, long orderId, String requestId) {
        if (!isOwned(accountId)) {
            log.log(Level.WARNING, "[계좌] 담당 슬롯이 아닌 계좌로 이벤트가 왔습니다(라우팅 확인 필요): accountId=" + accountId);
            listener.onRejected(accountId, orderId, requestId, RejectReason.NOT_OWNED);
            return true;
        }
        return false;
    }

    /**
     * 소비자가 실제로 처리한 시점의 체결 수신 위치를 sessionId별로 담은 맵(1-2, ADR-032 I1 D1) —
     * 아직 처리 안 된 체결은 반영하지 않는다. 호출부가 내부 맵을 직접 변경하지 못하게 복사본을
     * 돌려준다.
     */
    public Map<Integer, Long> lastAppliedFillPositions() {
        return Map.copyOf(lastAppliedFillPositions);
    }

    /** 소비자가 실제로 처리한 시점의 저널 위치(1-2). */
    public long lastJournaledPosition() {
        return lastJournaledPosition;
    }

    /**
     * 복구 직후(start() 전, 단일 스레드) 체결 수신 위치를 스냅샷이 가리키던 값으로 시드한다(1-3).
     * 안 하면 복구~첫 라이브 체결 사이에 러닝 중 스냅샷이 찍힐 때 그 sessionId의 위치가 빈 채로
     * 저장돼, 다음 복구가 그 발행자의 체결 스트림을 처음부터 다시 replay한다 — 그사이 이미
     * 가지치기된 tradeId가 있으면 이중 반영(돈)으로 이어질 수 있다.
     */
    public void seedLastAppliedFillPositions(Map<Integer, Long> fillPositions) {
        this.lastAppliedFillPositions.putAll(fillPositions);
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
        boolean offered = snapshotSink.offer(snapshotBytes, lastAppliedFillPositions(), lastJournaledPosition, appliedSeq);
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

        if (rejectIfNotOwned(accountId, NO_ORDER_ID, requestId)) {
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

        BigDecimal price = event.getPrice();
        if (event.getQuantity() <= 0 || price == null || price.signum() <= 0) {
            listener.onRejected(accountId, orderId, requestId, RejectReason.INVALID_QUANTITY);
            return;
        }

        ReserveResult result = state.tryReserve(orderId, price, event.getQuantity());
        if (result.accepted()) {
            latencyHistogram.record(event.getPublishedAtEpochNanos());
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

        if (rejectIfNotOwned(accountId, NO_ORDER_ID, requestId)) {
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
            latencyHistogram.record(event.getPublishedAtEpochNanos());
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
        lastAppliedFillPositions.put(event.getSourceSessionId(), event.getSourcePosition());

        if (rejectIfNotOwned(accountId, orderId, event.getRequestId())) {
            return;
        }
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
        lastAppliedFillPositions.put(event.getSourceSessionId(), event.getSourcePosition());

        if (rejectIfNotOwned(accountId, orderId, event.getRequestId())) {
            return;
        }
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

        if (rejectIfNotOwned(accountId, NO_ORDER_ID, event.getRequestId())) {
            return;
        }
        AccountState state = accounts.get(accountId);
        if (state == null) {
            listener.onRejected(accountId, NO_ORDER_ID, event.getRequestId(), RejectReason.ACCOUNT_NOT_FOUND);
            return;
        }
        boolean applied = state.applySettlement(settlementRef, event.getPrice());
        if (applied) {
            listener.onStateChanged(accountId, state.balance(), state.holdings(), state.seq());
        }
        listener.onSettlementApplied(accountId, settlementRef, applied);
    }
}
