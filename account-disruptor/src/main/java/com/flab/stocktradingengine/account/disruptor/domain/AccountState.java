package com.flab.stocktradingengine.account.disruptor.domain;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.disruptor.snapshot.AccountStateSnapshot;
import com.flab.stocktradingengine.account.disruptor.snapshot.BuyReservationSnapshot;
import com.flab.stocktradingengine.account.disruptor.snapshot.SellReservationSnapshot;
import com.flab.stocktradingengine.account.disruptor.snapshot.TradeIdGenerationSnapshot;

/**
 * 계좌 하나의 인메모리 상태.
 *
 * <p>소비자 스레드 하나만 이 객체를 만진다(single writer) — 그래서 락이 없다.
 * 검증·예약과 체결 반영을 이 객체가 직접 책임한다(Rich Domain).</p>
 *
 * <h3>예약 장부</h3>
 * <p>예약을 {@code orderId → Reservation(price, remainingQuantity)} 목록(장부)으로 든다.
 * 부분 체결이 오면 그 주문의 남은 수량만 줄여야 하므로, 총액이 아니라 가격·잔량을 따로 기억한다.
 * 예약 증거금은 그때그때 {@code price × remainingQuantity × marginRate}로 다시 계산한다
 * (부분 체결마다 누적 차감하지 않고 잔량 기준으로 매번 새로 계산해, 반올림이 누적되지 않는다).
 * 가용 계산에 쓰는 "예약 총합"은 장부 값들의 합이다(v1 {@code sumReservedMargin} 대체).</p>
 */
public final class AccountState {

    private final long accountId;
    // 총 현금 잔액. seed·매수체결(증거금분)·정산(applySettlement, 미수금분, T+2)에서 바뀐다.
    // 증거금 미수거래 모델: 체결 때 증거금분(fillAmount×marginRate)은 바로 지불(balance 차감)되고,
    // 나머지(1-marginRate)만 미수금으로 남아 T+2에 정산된다 — 매도(applySellFill)는 증거금 개념이
    // 없어 balance를 안 건드린다. single-writer라 락 없이 안전.
    private BigDecimal balance;
    private final BigDecimal marginRate;   // 증거금률 (0.40 ~ 1.00, 시드값 전제)
    private final Map<Long, Reservation> reservations = new HashMap<>();         // orderId → 매수 예약(가격·잔량) 장부
    private final Map<Long, SellReservation> sellReservations = new HashMap<>(); // orderId → 매도 예약(종목·잔량) 장부
    private final Map<String, Integer> holdings = new HashMap<>();      // 종목코드 → 보유 수량
    // 이미 반영한 체결(tradeId) 세대별 장부(1-4, docs/_tradeid_snapshot_prune.html) — 무상한 HashSet
    // 하나 대신 "저널 적용 순번(boundarySeq)마다 세대를 나눠 쌓고, durable 스냅샷 경계보다 오래된
    // 세대를 통째로 버린다"는 안 A(세대별 HashSet). 맨 앞(peekFirst)이 현재 세대 — 새 tradeId는
    // 항상 거기에만 추가한다. 판정은 모든 세대를 훑어 하나라도 있으면 중복이다. 세대 경계를 넘기고
    // (startNewGeneration) 가지치기(pruneOlderThan)하는 건 소비자 스레드(핸들러)의 몫이다(1-3) —
    // AccountState는 자료구조와 판정만 책임진다.
    private final Deque<TradeIdGeneration> tradeIdGenerations = new ArrayDeque<>();
    // AccountSettlementConsumer가 ack-mode=manual_immediate + enable-auto-commit=false라 정산 하나를
    // 저널에 기록한 뒤 바로 offset을 커밋한다 → 크래시 시 재소비되는 건 "커밋 직전 처리 중이던 1건"뿐이다.
    // max.poll.records를 따로 설정하지 않아 Kafka 기본값 500이라, 어떤 리밸런스·재조정이 겹쳐도 한 번의
    // poll 배치(≤500)를 넘는 재소비는 없다 — 그래서 복구 겹침 상한을 500(=max.poll.records)으로 잡는다.
    // ⚠️ 이 500은 max.poll.records 기본값에 묶인 값이다. 그 설정을 500보다 크게 올리면 dedup 창이
    //    한 poll 배치보다 작아져 복구 겹침에서 멱등이 뚫릴 수 있다 → 설정을 올릴 때 이 상한도 함께 올릴 것.
    static final int SETTLEMENT_RETENTION_LIMIT = 500;
    // 이미 반영한 정산(settlementRef), 멱등용. 상한 없는 HashSet은 장기 실행 시 무한 증가하므로(OOM 릭)
    // OrderBook.filledOrderTimestamps와 같은 방식(LinkedHashMap 삽입순서 + removeEldestEntry)으로 상한을 둔다.
    // 삽입순서 퇴출(FIFO — accessOrder=false라 먼저 들어온 것부터 밀려난다)이 이 복구 겹침 상한(500)을
    // 덮으므로, 밀려난 정산이 재도착해 멱등이 뚫릴 일은 없다.
    private final Set<Long> processedSettlementRefs;
    // ⚠️ 이 값은 근거 있는 상한이 아니라 릭(무한증가) 방지용 임시값이다. requestId 상한이 덮어야
    // 하는 "재전송 창"은 클라 재시도 정책·결과 폴링 조회 API(fork5 ⑤ 후속)에 묶이는데 둘 다
    // 미구현이라 지금 근거를 못 댄다. 폴링 조회 API 설계 시 그 창 크기로 재산정할 것.
    static final int REQUEST_ID_RETENTION_LIMIT = 5000;
    // 이미 처리한 매수·매도 접수 requestId, 재전송 멱등용(C5-1a). orderId는 안 담는다 — 재전송의
    // 원래 orderId를 실제로 쓰는 소비자가 없었다(릭 수정 U2, 로그 한 줄뿐이고 클라 응답 경로로도
    // 안 나감). settlementRefs와 같은 삽입순서 상한 Set(FIFO bounded).
    private final Set<String> processedRequestIds;
    private BigDecimal unpaid = BigDecimal.ZERO;                        // 미결제 미수금
    // 계좌별 단조 카운터(계좌 상태 영속/프로젝션 트랙 Unit 1). 상태를 실제로 바꾸는 연산마다
    // 1씩 증가한다 — 나중에 이 계좌의 full-state를 캡처해 발행할 때 stale-guard(더 큰 seq만
    // 반영) 키로 쓴다. gap은 무해하므로(엄격 증가만 보장하면 됨) "모든 변경마다 +1"로 단순하게 간다.
    private long seq = 0L;

    public AccountState(long accountId, BigDecimal balance, BigDecimal marginRate) {
        this(accountId, balance, marginRate, SETTLEMENT_RETENTION_LIMIT, REQUEST_ID_RETENTION_LIMIT);
    }

    /** 초기 보유(종목코드 → 수량)를 함께 시드한다. DB 없이 기존 보유를 미리 넣을 때 쓴다. */
    public AccountState(long accountId, BigDecimal balance, BigDecimal marginRate, Map<String, Integer> initialHoldings) {
        this(accountId, balance, marginRate);
        holdings.putAll(initialHoldings);
    }

    /** 테스트 전용. settlementRetentionLimit을 작게 지정해 정산 멱등 캐시의 FIFO(삽입순서) 퇴출 동작을 검증할 때 쓴다. */
    AccountState(long accountId, BigDecimal balance, BigDecimal marginRate, int settlementRetentionLimit) {
        this(accountId, balance, marginRate, settlementRetentionLimit, REQUEST_ID_RETENTION_LIMIT);
    }

    /** 테스트 전용. settlement·requestId 멱등 캐시 상한을 모두 작게 지정해 FIFO(삽입순서) 퇴출 동작을 검증할 때 쓴다. */
    AccountState(long accountId, BigDecimal balance, BigDecimal marginRate, int settlementRetentionLimit, int requestIdRetentionLimit) {
        this.accountId = accountId;
        this.balance = balance;
        this.marginRate = marginRate;
        this.processedSettlementRefs = boundedSet(settlementRetentionLimit);
        this.processedRequestIds = boundedSet(requestIdRetentionLimit);
        tradeIdGenerations.addFirst(new TradeIdGeneration(0L, new HashSet<>()));
    }

    /**
     * 스냅샷(2d-2)에서 계좌 상태를 통째로 복원한다. {@code accountId}·{@code marginRate}가
     * final이라 기존 인스턴스를 고쳐 쓸 수 없어, seed 생성자와 별개로 스냅샷 값 그대로 새
     * 인스턴스를 만든다 — {@link AccountEngine#restore}가 이 생성자로 만든 인스턴스를 seed가
     * 만든 것과 그대로 바꿔 끼운다.
     */
    public AccountState(AccountStateSnapshot snapshot) {
        this(snapshot.accountId(), snapshot.balance(), snapshot.marginRate(), SETTLEMENT_RETENTION_LIMIT, REQUEST_ID_RETENTION_LIMIT);
        this.unpaid = snapshot.unpaid();
        this.seq = snapshot.seq();
        holdings.putAll(snapshot.holdings());
        // 생성자 초기값(세대 1개)을 스냅샷의 세대들로 통째로 바꿔 끼운다 — 순서(현재 세대가 0번째)를
        // 그대로 보존해야 이후 startNewGeneration·pruneOlderThan이 같은 경계로 이어진다.
        tradeIdGenerations.clear();
        for (TradeIdGenerationSnapshot generation : snapshot.tradeIdGenerations()) {
            tradeIdGenerations.addLast(new TradeIdGeneration(generation.boundarySeq(), new HashSet<>(generation.tradeIds())));
        }
        processedSettlementRefs.addAll(snapshot.processedSettlementRefs());
        processedRequestIds.addAll(snapshot.processedRequestIds());
        snapshot.reservations().forEach((orderId, r) ->
            reservations.put(orderId, new Reservation(r.price(), r.remainingQuantity())));
        snapshot.sellReservations().forEach((orderId, r) ->
            sellReservations.put(orderId, new SellReservation(r.stockCode(), r.remainingQuantity())));
    }

    /** 삽입순서 상한 집합(FIFO bounded) — OrderBook.filledOrderTimestamps와 같은 패턴. */
    private static <T> Set<T> boundedSet(int limit) {
        Map<T, Boolean> boundedMap = new LinkedHashMap<>(limit, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<T, Boolean> eldest) {
                return size() > limit;
            }
        };
        return Collections.newSetFromMap(boundedMap);
    }

    /**
     * tradeId가 이미 반영된 적 있는지 판정한다(1-4). 모든 세대를 훑어 하나라도 있으면 중복이다.
     * 처음 보는 tradeId면 현재 세대(맨 앞, {@link Deque#peekFirst()})에만 기록한다 — 옛 세대는
     * 절대 건드리지 않는다(가지치기로만 줄어든다).
     */
    private boolean isDuplicateTradeId(long tradeId) {
        for (TradeIdGeneration generation : tradeIdGenerations) {
            if (generation.tradeIds().contains(tradeId)) {
                return true;
            }
        }
        tradeIdGenerations.peekFirst().tradeIds().add(tradeId);
        return false;
    }

    /**
     * tradeId 멱등 장부의 새 세대를 연다(1-3이 저널 적용 순번이 N의 배수가 될 때마다 호출). 지금
     * 세대는 "직전 세대"로 밀리고, 이후 반영되는 tradeId는 새 세대에 쌓인다.
     *
     * @param boundarySeq 새 세대가 시작되는 시점의 저널 적용 순번 — 스냅샷이 durable해진 뒤 이
     *                    값보다 오래된(두 세대 전) tradeId를 가지치기해도 안전한 근거가 된다.
     */
    public void startNewGeneration(long boundarySeq) {
        tradeIdGenerations.addFirst(new TradeIdGeneration(boundarySeq, new HashSet<>()));
    }

    /**
     * 가장 오래된 세대부터 {@code keep}개를 넘는 세대를 버린다(1-3이 스냅샷 durable 확인 뒤 호출).
     * 현재 세대 + 직전 세대(기본 {@code keep=2})는 항상 남겨 재전송 창을 덮는다.
     */
    public void pruneOlderThan(int keep) {
        while (tradeIdGenerations.size() > keep) {
            tradeIdGenerations.removeLast();
        }
    }

    /** tradeId 멱등 장부 세대 한 줄 — 세대 시작 저널 적용 순번과 그 세대에 쌓인 tradeId 집합. */
    private record TradeIdGeneration(long boundarySeq, Set<Long> tradeIds) {
    }

    /** 현재 상태를 스냅샷으로 찍는다(2d-2). {@link #reservations}·{@link #sellReservations}는
     *  private record라 공개 서브레코드({@link BuyReservationSnapshot}·{@link SellReservationSnapshot})로 옮겨 담는다. */
    public AccountStateSnapshot toSnapshot() {
        Map<Long, BuyReservationSnapshot> reservationSnapshots = new HashMap<>();
        reservations.forEach((orderId, r) -> reservationSnapshots.put(orderId, new BuyReservationSnapshot(r.price(), r.remainingQuantity())));

        Map<Long, SellReservationSnapshot> sellReservationSnapshots = new HashMap<>();
        sellReservations.forEach((orderId, r) -> sellReservationSnapshots.put(orderId, new SellReservationSnapshot(r.stockCode(), r.remainingQuantity())));

        List<TradeIdGenerationSnapshot> tradeIdGenerationSnapshots = new ArrayList<>();
        for (TradeIdGeneration generation : tradeIdGenerations) {
            tradeIdGenerationSnapshots.add(new TradeIdGenerationSnapshot(generation.boundarySeq(), Set.copyOf(generation.tradeIds())));
        }

        return new AccountStateSnapshot(
            accountId, seq, balance, marginRate,
            reservationSnapshots, sellReservationSnapshots,
            Map.copyOf(holdings), List.copyOf(tradeIdGenerationSnapshots), Set.copyOf(processedSettlementRefs),
            Set.copyOf(processedRequestIds), unpaid);
    }

    /**
     * 이 requestId가 재전송인지 판정하면서, 동시에 처음 보는 것이면 장부에 기록한다(C5-1a).
     *
     * <p>매수·매도 접수(handleBuy·handleSell) 맨 앞에서 호출한다 — {@link Set#add}가 이미 있으면
     * false를 돌려주는 것을 그대로 이용해 조회와 기록을 한 번에 한다. accept·reject 결과와
     * 무관하게 첫 등장에서 한 번만 장부에 남는다 — 거부된 주문도 재전송이면 재예약하지 않는다.</p>
     *
     * @return 이미 처리한 적 있는(재전송) requestId면 true, 처음 보는 것이면 false
     */
    public boolean isDuplicateRequest(String requestId) {
        return !processedRequestIds.add(requestId);
    }

    /**
     * 매수 주문을 검증하고, 통과하면 그 주문의 예약(가격·수량)을 장부에 기록한다.
     *
     * @param orderId  주문 신원(장부 키) — 나중에 체결 시 이 키로 예약을 푼다
     * @param price    주문 가격(지정가) — 예약 증거금 계산의 기준
     * @param quantity 주문 수량
     * @return 통과면 accepted(예약 증거금), 초과면 rejected(INSUFFICIENT)
     */
    public ReserveResult tryReserve(long orderId, BigDecimal price, int quantity) {
        BigDecimal orderAmount = price.multiply(BigDecimal.valueOf(quantity));
        BigDecimal reservedThis = reservedAmount(price, quantity);
        BigDecimal withdrawable = balance.subtract(totalReserved()).subtract(unpaid);
        BigDecimal buyLimit = withdrawable.divide(marginRate, 0, RoundingMode.DOWN);

        if (orderAmount.compareTo(buyLimit) > 0) {
            return ReserveResult.rejected(RejectReason.INSUFFICIENT);
        }

        reservations.put(orderId, new Reservation(price, quantity));
        seq++;
        return ReserveResult.accepted(reservedThis);
    }

    /**
     * 매수 체결 반영(전량·부분 공통): 체결된 수량만큼 그 주문의 예약을 줄이고, 보유를 늘리고,
     * 증거금분을 balance에서 차감하고, 나머지를 미수금으로 쌓는다. 잔량이 남으면 예약을 유지하고,
     * 0이 되면 장부에서 지운다. (미수금분은 T+2 결제 — {@link #applySettlement} 참고.)
     *
     * @param tradeId 체결 신원(멱등키) — 이미 반영한 tradeId 면 아무것도 하지 않고 무시한다
     * @throws IllegalStateException 그 orderId 로 예약된 게 없거나, 체결 수량이 남은 예약 수량을 초과하면
     *                                (매칭이 검증 안 된 주문을 체결시킨 것이므로 도메인 불변식 위반)
     * @return 이번 호출로 실제 반영했으면 true, 이미 반영한 tradeId 라 무시했으면 false
     */
    public BuyFillResult applyBuyFill(long tradeId, long orderId, String stockCode, BigDecimal matchPrice, int fillQty) {
        if (isDuplicateTradeId(tradeId)) {
            return BuyFillResult.notApplied(); // 이미 반영한 체결 재도착 — 무시
        }
        Reservation reservation = reservations.get(orderId);
        if (reservation == null) {
            throw new IllegalStateException("예약되지 않은 주문에 대한 체결입니다: orderId=" + orderId);
        }
        int remainingQuantity = reservation.remainingQuantity() - fillQty;
        if (remainingQuantity < 0) {
            throw new IllegalStateException("체결 수량이 남은 예약 수량을 초과합니다: orderId=" + orderId
                + " 남은수량=" + reservation.remainingQuantity() + " 체결수량=" + fillQty);
        }
        // 이 체결로 예약이 얼마나 줄어드는지를 먼저 스냅샷 찍는다(반올림 전/후 값의 차).
        // 증거금분(marginPaid)을 fillAmount×marginRate로 따로 계산하지 않고 이 차이로 구하는 이유:
        // 부분체결이 여러 번 이어지면 각 체결의 "예약액 감소분"들을 다 더한 값이 최초 예약액과
        // 원 단위까지 정확히 같아야 한다(텔레스코핑) — 따로 계산하면 반올림이 어긋나 누적된다.
        BigDecimal reservedBefore = reservedAmount(reservation.price(), reservation.remainingQuantity());
        BigDecimal reservedAfter;
        if (remainingQuantity == 0) {
            reservations.remove(orderId);
            reservedAfter = BigDecimal.ZERO;
        } else {
            reservations.put(orderId, new Reservation(reservation.price(), remainingQuantity));
            reservedAfter = reservedAmount(reservation.price(), remainingQuantity);
        }
        // 보유 추가
        holdings.merge(stockCode, fillQty, Integer::sum);
        // 증거금 미수거래 모델: 체결액 중 증거금분은 이 시점에 실제로 지불되고(balance 차감),
        // 나머지는 미수금으로 남아 T+2(applySettlement)에 정산된다.
        BigDecimal fillAmount = matchPrice.multiply(BigDecimal.valueOf(fillQty));
        BigDecimal marginPaid = reservedBefore.subtract(reservedAfter);
        if (marginPaid.compareTo(fillAmount) > 0) {
            // matchPrice가 예약 당시 price와 달라 이론상 역전될 수 있는 극단값 방어(돈 관련 안전장치) —
            // 증거금분이 체결액 전체를 넘을 순 없다.
            marginPaid = fillAmount;
        }
        BigDecimal unpaidThis = fillAmount.subtract(marginPaid);
        balance = balance.subtract(marginPaid);
        unpaid = unpaid.add(unpaidThis);
        seq++;
        return new BuyFillResult(true, unpaidThis);
    }

    /**
     * 매도 주문을 검증하고, 통과하면 그 주문의 매도 예약(보유 수량 담보)을 장부에 기록한다.
     * 매수의 {@link #tryReserve}와 대칭이지만 담보가 돈이 아니라 보유 수량이다.
     *
     * @param orderId  주문 신원(장부 키) — 나중에 체결 시 이 키로 예약을 푼다
     * @param quantity 매도 수량
     * @return 통과면 accepted(예약 수량), 초과면 rejected(INSUFFICIENT_HOLDING)
     */
    public SellReserveResult trySellReserve(long orderId, String stockCode, int quantity) {
        int available = holding(stockCode) - reservedSellQuantity(stockCode);
        if (quantity > available) {
            return SellReserveResult.rejected(RejectReason.INSUFFICIENT_HOLDING);
        }
        sellReservations.put(orderId, new SellReservation(stockCode, quantity));
        seq++;
        return SellReserveResult.accepted(quantity);
    }

    /**
     * 매도 체결 반영(전량·부분 공통): 체결된 수량만큼 그 주문의 매도 예약을 줄이고 보유를 줄인다.
     * 잔량이 남으면 예약을 유지하고, 0이 되면 장부에서 지운다.
     *
     * @param tradeId 체결 신원(멱등키) — 이미 반영한 tradeId 면 아무것도 하지 않고 무시한다
     * @throws IllegalStateException 그 orderId 로 매도 예약된 게 없거나, 체결 수량이 남은 예약 수량을 초과하면
     * @return 이번 호출로 실제 반영했으면 true, 이미 반영한 tradeId 라 무시했으면 false
     */
    public boolean applySellFill(long tradeId, long orderId, String stockCode, int fillQty) {
        if (isDuplicateTradeId(tradeId)) {
            return false; // 이미 반영한 체결 재도착 — 무시
        }
        SellReservation reservation = sellReservations.get(orderId);
        if (reservation == null) {
            throw new IllegalStateException("매도 예약되지 않은 주문에 대한 체결입니다: orderId=" + orderId);
        }
        int remainingQuantity = reservation.remainingQuantity() - fillQty;
        if (remainingQuantity < 0) {
            throw new IllegalStateException("체결 수량이 남은 매도 예약 수량을 초과합니다: orderId=" + orderId
                + " 남은수량=" + reservation.remainingQuantity() + " 체결수량=" + fillQty);
        }
        if (remainingQuantity == 0) {
            sellReservations.remove(orderId);
        } else {
            sellReservations.put(orderId, new SellReservation(reservation.stockCode(), remainingQuantity));
        }
        holdings.merge(stockCode, -fillQty, Integer::sum);
        seq++;
        return true;
    }

    /**
     * 정산(T+2) 되돌림을 반영한다: 잔고를 amount 만큼 깎고 미수금을 같은 만큼 줄인다.
     * v2 신규 경로 — settlement가 "실제 잔고를 차감하라"고 보내는 명령이다(v1엔 없었다).
     *
     * @param settlementRef 정산 신원(멱등키) — 이미 반영한 settlementRef 면 아무것도 하지 않고 무시한다
     * @throws IllegalStateException amount 가 남은 미수금을 초과하면(정상 흐름이면 일어날 수 없는
     *                                도메인 불변식 위반 — ADR-015와 같은 원칙으로 fail-fast)
     * @return 이번 호출로 실제 반영했으면 true, 이미 반영한 settlementRef 라 무시했으면 false
     */
    public boolean applySettlement(long settlementRef, BigDecimal amount) {
        if (!processedSettlementRefs.add(settlementRef)) {
            return false; // 이미 반영한 정산 재도착 — 무시
        }
        if (amount.compareTo(unpaid) > 0) {
            throw new IllegalStateException("정산 금액이 미수금을 초과합니다: settlementRef=" + settlementRef
                + " 미수금=" + unpaid + " 정산금액=" + amount);
        }
        balance = balance.subtract(amount);
        unpaid = unpaid.subtract(amount);
        seq++;
        return true;
    }

    private BigDecimal totalReserved() {
        BigDecimal sum = BigDecimal.ZERO;
        for (Reservation reservation : reservations.values()) {
            sum = sum.add(reservedAmount(reservation.price(), reservation.remainingQuantity()));
        }
        return sum;
    }

    /**
     * UP(올림)으로 반올림한다 — DOWN이면 부분체결이 여러 번 이어질 때 {@link #applyBuyFill}이
     * "이번 체결로 줄어든 예약액"만큼만 증거금을 떼는데, 그 예약액 자체가 매번 DOWN으로 깎여
     * 나와서 실제로 뗀 증거금 합이 최초 예약액보다 커질 수 있었다(검증 안 된 만큼 더 빠져나감).
     * UP으로 두면 예약이 항상 "이후 실제로 뗄 금액" 이상이 되어 이 역전이 안 생긴다.
     */
    private BigDecimal reservedAmount(BigDecimal price, int quantity) {
        return price.multiply(BigDecimal.valueOf(quantity)).multiply(marginRate).setScale(0, RoundingMode.UP);
    }

    /** 주문 예약 장부 한 줄 — 가격과 남은(미체결) 수량. 예약 증거금은 이 둘로 매번 다시 계산한다. */
    private record Reservation(BigDecimal price, int remainingQuantity) {
    }

    /** 특정 종목의 현재 매도 예약 수량 총합(장부 값의 합). */
    public int reservedSellQuantity(String stockCode) {
        int sum = 0;
        for (SellReservation reservation : sellReservations.values()) {
            if (reservation.stockCode().equals(stockCode)) {
                sum += reservation.remainingQuantity();
            }
        }
        return sum;
    }

    /** 매도 예약 장부 한 줄 — 종목코드와 남은(미체결) 수량. */
    private record SellReservation(String stockCode, int remainingQuantity) {
    }

    public long accountId() {
        return accountId;
    }

    /** 현재 예약 증거금 총합(장부 값의 합). */
    public BigDecimal reservedMargin() {
        return totalReserved();
    }

    public int holding(String stockCode) {
        return holdings.getOrDefault(stockCode, 0);
    }

    /** 현재 보유 전체의 불변 스냅샷(계좌 상태 영속/프로젝션 트랙 Unit 2) — full-state 캡처용. */
    public Map<String, Integer> holdings() {
        return Map.copyOf(holdings);
    }

    public BigDecimal unpaid() {
        return unpaid;
    }

    public BigDecimal balance() {
        return balance;
    }

    /** 계좌별 단조 카운터(계좌 상태 영속/프로젝션 트랙 Unit 1) — full-state 캡처의 stale-guard 키. */
    public long seq() {
        return seq;
    }
}
