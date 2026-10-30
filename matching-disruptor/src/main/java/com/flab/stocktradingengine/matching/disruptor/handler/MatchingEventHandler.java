package com.flab.stocktradingengine.matching.disruptor.handler;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.lmax.disruptor.EventHandler;

import com.flab.stocktradingengine.aeron.StockSlotHasher;
import com.flab.stocktradingengine.trading.matching.FillResult;
import com.flab.stocktradingengine.trading.matching.OrderBook;
import com.flab.stocktradingengine.trading.matching.OrderEntry;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.time.EpochNanos;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingSegmentStages;
import com.flab.stocktradingengine.matching.disruptor.engine.OrderEvent;
import com.flab.stocktradingengine.matching.disruptor.io.MatchListener;
import com.flab.stocktradingengine.matching.disruptor.io.MatchingSnapshotSink;
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventListener;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshot;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshotCodec;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshotFactory;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshotRestorer;

/**
 * 링버퍼를 소비하는 단일 매칭 핸들러.
 *
 * <h3>단일 스레드 보장</h3>
 * <p>Disruptor 가 이 핸들러를 스레드 하나에만 배정하므로, 여기서 만지는 상태는
 * 한 스레드만 접근한다. 그래서 종목별 호가창을 담는 {@code books} 를 동시성 구조가 아닌
 * 일반 {@link HashMap} 으로 둔다. AS-IS 의 {@code OrderBookRegistry}(ConcurrentHashMap)와
 * 달리 동시 접근이 없어 락·동시성 맵이 필요 없다.</p>
 *
 * <h3>매칭 로직</h3>
 * <p>주문 접수·취소·체결 판단은 기존 {@link OrderBook} 을 그대로 재사용한다.
 * 이 핸들러는 링버퍼 이벤트를 {@link OrderEntry} 로 옮겨 OrderBook 에 넘기고,
 * 체결이 나오면 {@link MatchListener} 로 내보내는 얇은 껍데기다.</p>
 *
 * <h3>러닝 중 스냅샷(I6 U1)</h3>
 * <p>저널 적용 순번({@link #appliedSeq})이 N건({@link #SNAPSHOT_INTERVAL_JOURNAL_ENTRIES})에
 * 도달할 때마다 이 스레드가 직접 books 를 직렬화해(단일 스레드만 접근하는 시점이라 안전)
 * {@link #snapshotSink} 에 넘긴다. 실제 디스크 쓰기는 싱크 구현체의 별도 스레드가 한다(ADR-024
 * "single-writer는 I/O 안 함") — account-disruptor {@code AccountEventHandler} 와 같은 구조.</p>
 */
public class MatchingEventHandler implements EventHandler<OrderEvent> {

    private static final Logger log = System.getLogger(MatchingEventHandler.class.getName());

    private static final MatchingSnapshotSink NO_OP_SINK = new MatchingSnapshotSink() {
        @Override
        public boolean offer(byte[] snapshotBytes, Map<Integer, Long> orderIntakePositions, long appliedSeq) {
            return true;
        }

        @Override
        public long durableSeq() {
            return 0L;
        }
    };

    // 저널 N건마다 러닝 중 스냅샷을 찍는다(I6 D1) — 계좌(AccountEventHandler)와 같은 임시값.
    // 매칭은 이벤트 성격이 달라(주문 유입) 빈도가 다를 수 있어 실측 재산정을 후속으로 남긴다.
    private static final long SNAPSHOT_INTERVAL_JOURNAL_ENTRIES = 10_000L;
    // 종목 slot 개수 기본값(matching-shard.stock-slot-count의 기본값과 같다). 종목 slot을 모르는 기존 구성이 쓴다.
    private static final int DEFAULT_STOCK_SLOT_COUNT = 64;

    // 종목코드 → 호가창. 단일 스레드만 접근하므로 일반 HashMap 으로 충분하다.
    // MatchingEngine 이 필드로 들고 있다가 넘겨준다 — recover(2c-2)가 새 핸들러 인스턴스로
    // 저널을 재적용할 때도 같은 맵을 공유해야 라이브 매칭이 그 위에서 이어진다
    // (account-disruptor AccountEventHandler 의 accounts 맵과 같은 이유).
    private final Map<String, OrderBook> books;
    // 전량 체결되어 호가창을 떠난 주문 수의 누적(12-3절 1번 표의 「바」). 이 프로세스가 시작한 뒤 저널을 다시 적용하며 낸 체결도 센다.
    private final java.util.concurrent.atomic.LongAdder fullyFilledOrders = new java.util.concurrent.atomic.LongAdder();
    private final MatchListener listener;
    private final MatchingSegmentStages stages;
    private final MatchingSnapshotSink snapshotSink;
    private final boolean snapshotTriggerEnabled;
    private final MatchingSnapshotCodec snapshotCodec = new MatchingSnapshotCodec();
    private final StockSlotHasher stockSlotHasher;
    private final SlotEventListener slotEventListener;
    // 넘김 이벤트에 도달해 넘기는 중인 종목 slot. 이 slot의 종목은 이후 PLACE·CANCEL을 호가창에 반영하지 않는다(U13).
    // 이 스레드만 접근한다. 비어 있으면 주문마다 종목 slot을 계산하지 않는다.
    private final Set<Integer> handedOverSlots = new HashSet<>();
    // 받음이 반영되고 넘김이 반영되지 않은 종목 slot. 이 종목 slot에 도착한 호가창 적재는 낡은 사본이라 적용하지 않는다.
    private final Set<Integer> acquiredSlots = new HashSet<>();

    // 적재를 마치고 강제 스냅샷의 디스크 확정을 기다리는 호가창 이동(U15). 엔진 thread가 넣고 완료를 확인하는 thread가 꺼낸다.
    // appliedSeq가 -1이면 쓰기 queue가 차서 강제 스냅샷이 거부되어 아직 다시 찍어야 한다.
    private final Queue<PendingLoad> loadsAwaitingDurable = new ConcurrentLinkedQueue<>();
    // 강제 스냅샷 재시도 이벤트를 링버퍼에 넣어 두었고 엔진 thread가 아직 처리하지 않았는지.
    private final AtomicBoolean forceSnapshotRetryQueued = new AtomicBoolean();
    // 거부된 강제 스냅샷을 다시 찍는 간격(10ms, 근거 없는 임의 값 — 스냅샷 쓰기 하나가 fsync까지 수 ms~수십 ms 걸릴 수 있어 그 정도로 둔다).
    private static final long FORCE_SNAPSHOT_RETRY_INTERVAL_NANOS = 10_000_000L;
    // 강제 스냅샷을 마지막으로 시도한 시각. 엔진 thread가 쓰고 수신 thread가 읽는다.
    private volatile long lastForcedSnapshotAttemptNanos = System.nanoTime();

    private static final class PendingLoad {
        private final long transferId;
        private final int stockSlot;
        private volatile long appliedSeq;

        private PendingLoad(long transferId, int stockSlot, long appliedSeq) {
            this.transferId = transferId;
            this.stockSlot = stockSlot;
            this.appliedSeq = appliedSeq;
        }
    }

    // JournalEventHandler가 슬롯에 실어준 값(I6 U1) — 이 핸들러(소비자 스레드)만 읽는다.
    private long lastJournaledPosition;
    // 저널 적용 순번(I6 U1) — 처리 결과와 무관하게 이벤트 하나를 소비할 때마다 1씩 증가한다.
    private long appliedSeq;
    // 소비자가 실제로 반영한 주문의 인테이크 수신 위치를 발행자(계좌 샤드 Aeron sessionId)별로
    // 담는다(I2 U1, account-disruptor AccountEventHandler.lastAppliedFillPositions과 같은 이유).
    // 스냅샷 쓰기 스레드가 takeSnapshot() 호출 시점에 이 맵을 읽어가므로(다른 스레드) 동시성 맵.
    private final Map<Integer, Long> lastAppliedOrderIntakePositions = new ConcurrentHashMap<>();

    // 이 배치에서 건드린 종목. 배치의 마지막 이벤트에서 종목마다 한 번 listener 에 호가창 변경을 알리고 비운다 — 이 스레드만 접근한다
    private final Set<String> touchedInBatch = new HashSet<>();

    public MatchingEventHandler(Map<String, OrderBook> books, MatchListener listener) {
        this(books, listener, NO_OP_SINK, false);
    }

    /**
     * @param snapshotTriggerEnabled N건마다 스냅샷을 직렬화+offer할지. {@link
     *     com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine#recover}의 replay
     *     전용 핸들러는 항상 false로 둔다(계좌 replay 핸들러와 같은 이유) — 이미 지나간 저널을
     *     다시 훑는 것뿐이라 새로 찍을 스냅샷이 없다.
     */
    public MatchingEventHandler(Map<String, OrderBook> books, MatchListener listener,
                                MatchingSnapshotSink snapshotSink, boolean snapshotTriggerEnabled) {
        this(books, listener, snapshotSink, snapshotTriggerEnabled, MatchingSegmentStages.DISABLED);
    }

    /** 구간 분해 측정(W-P13·W-P14)용 단계 히스토그램 묶음을 함께 받는다. */
    public MatchingEventHandler(Map<String, OrderBook> books, MatchListener listener,
                                MatchingSnapshotSink snapshotSink, boolean snapshotTriggerEnabled,
                                MatchingSegmentStages stages) {
        this(books, listener, snapshotSink, snapshotTriggerEnabled, stages,
            new StockSlotHasher(DEFAULT_STOCK_SLOT_COUNT), SlotEventListener.NO_OP);
    }

    /** 종목 slot 받음·넘김 이벤트(U13)를 처리하는 데 쓰는 종목 slot 변환기와 통지를 함께 받는다. */
    public MatchingEventHandler(Map<String, OrderBook> books, MatchListener listener,
                                MatchingSnapshotSink snapshotSink, boolean snapshotTriggerEnabled,
                                MatchingSegmentStages stages,
                                StockSlotHasher stockSlotHasher, SlotEventListener slotEventListener) {
        this.stockSlotHasher = stockSlotHasher;
        this.slotEventListener = slotEventListener;
        this.stages = stages;
        this.books = books;
        this.listener = listener;
        this.snapshotSink = snapshotSink;
        this.snapshotTriggerEnabled = snapshotTriggerEnabled;
    }

    @Override
    public void onEvent(OrderEvent event, long sequence, boolean endOfBatch) {
        if (event.isPositionOnly()) {
            // 거부한 주문의 위치만 반영한다. 호가창과 스냅샷 주기 계산(appliedSeq)은 건드리지 않는다.
            lastAppliedOrderIntakePositions.put(event.getSourceSessionId(), event.getSourcePosition());
            lastJournaledPosition = event.getJournaledPosition();
            event.clear();
            if (endOfBatch) {
                notifyBooksChanged();
            }
            return;
        }
        if (event.isBookLoad() || event.isForceSnapshot() || event.isSlotCleanup()) {
            lastJournaledPosition = event.getJournaledPosition();
            if (event.isBookLoad()) {
                handleBookLoad(event);
            } else if (event.isSlotCleanup()) {
                handleSlotCleanup(event);
            } else {
                // 시도를 끝내고 시각을 갱신한 뒤에 표시를 푼다. 먼저 풀면 시도 중에 수신 thread가 간격이 지난 것으로 보고 한 번 더 발행한다.
                retryRejectedForcedSnapshots();
                forceSnapshotRetryQueued.set(false);
            }
            event.clear();
            if (endOfBatch) {
                notifyBooksChanged();
            }
            appliedSeq++;
            return;
        }
        if (event.isSlotEvent()) {
            lastJournaledPosition = event.getJournaledPosition();
            handleSlotEvent(event);
            event.clear();
            if (endOfBatch) {
                notifyBooksChanged();
            }
            completeApplied();
            return;
        }
        if (stages.isEnabled()) {
            long businessStartedAtNanos = EpochNanos.now();
            event.setBusinessStartedAtNanos(businessStartedAtNanos);
            stages.recordArrival(event, businessStartedAtNanos);
        }
        lastJournaledPosition = event.getJournaledPosition();
        String stockCode = event.getStockCode();   // 아래 finally 에서 슬롯을 비우기 전에 읽어 둔다
        boolean handedOver = isHandedOver(stockCode);
        try {
            if (handedOver) {
                log.log(Level.WARNING, "[매칭] 넘기는 중인 종목 slot의 이벤트를 호가창에 반영하지 않음: type=" + event.getType()
                    + " orderId=" + event.getOrderId() + " 종목=" + stockCode);
            } else if (event.getType() == EventType.PLACE) {
                handlePlace(event);
            } else if (event.getType() == EventType.CANCEL) {
                handleCancel(event);
            }
        } catch (IllegalArgumentException | IllegalStateException e) {
            // 도메인 불변식 위반(수량 초과·잘못된 상태 전이 등)은 재시도해도 같으므로 이 이벤트만 폐기한다.
            // 핸들러에서 예외를 밖으로 던지면 Disruptor 기본 처리기가 시퀀스를 멈추므로 여기서 잡아 넘긴다.
            log.log(Level.WARNING, "[매칭] 이벤트 폐기: type=" + event.getType()
                + " orderId=" + event.getOrderId() + " 이유=" + e.getMessage());
        } finally {
            // 슬롯 재사용 대비: 이 핸들러가 마지막 소비자이므로 처리 후 비운다.
            event.clear();
        }
        if (stockCode != null && !handedOver) {
            touchedInBatch.add(stockCode);
        }
        if (endOfBatch) {
            notifyBooksChanged();
        }
        completeApplied();
    }

    /** 저널 적용 순번을 올리고, N건째면 러닝 중 스냅샷을 찍는다. 위치만 담은 이벤트는 이 경로를 지나지 않는다. */
    private void completeApplied() {
        appliedSeq++;
        if (snapshotTriggerEnabled && appliedSeq % SNAPSHOT_INTERVAL_JOURNAL_ENTRIES == 0) {
            takeSnapshot();
        }
    }

    private boolean isHandedOver(String stockCode) {
        return !handedOverSlots.isEmpty() && stockCode != null
            && handedOverSlots.contains(stockSlotHasher.slotFor(stockCode));
    }

    /**
     * 받음은 그 종목 slot을 넘기는 중 목록에서 뺀다(다시 받은 종목 slot은 주문을 반영한다). 넘김은 그 종목 slot에 속한
     * 종목들의 호가창을 이 시점의 모습 그대로 찍어 인코딩하고, 넘기는 중 목록에 넣은 뒤 바이트를 통지한다.
     * 호가창 자체를 지우는 일은 하지 않는다(넘기기가 끝난 뒤의 일).
     */
    private void handleSlotEvent(OrderEvent event) {
        int stockSlot = event.getStockSlot();
        if (event.getType() == EventType.SLOT_ACQUIRED) {
            handedOverSlots.remove(stockSlot);
            acquiredSlots.add(stockSlot);
            return;
        }
        acquiredSlots.remove(stockSlot);
        Map<String, OrderBook> slotBooks = new TreeMap<>();
        for (Map.Entry<String, OrderBook> entry : books.entrySet()) {
            if (stockSlotHasher.slotFor(entry.getKey()) == stockSlot) {
                slotBooks.put(entry.getKey(), entry.getValue());
            }
        }
        handedOverSlots.add(stockSlot);
        List<byte[]> bookBytesPerStock = new ArrayList<>(slotBooks.size());
        for (Map.Entry<String, OrderBook> entry : slotBooks.entrySet()) {
            bookBytesPerStock.add(snapshotCodec.encode(
                MatchingSnapshotFactory.capture(Map.of(entry.getKey(), entry.getValue()), lastJournaledPosition)));
        }
        slotEventListener.onSlotHandedOver(stockSlot, bookBytesPerStock);
    }

    /** 넘기는 중인 종목 slot의 사본. 복구가 앞 단계에서 만든 목록을 다음 단계의 핸들러로 이어 줄 때 쓴다. 엔진이 시작되기 전에만 부른다. */
    public Set<Integer> handedOverSlotsSnapshot() {
        return Set.copyOf(handedOverSlots);
    }

    /** 복구가 이미 알아낸 넘기는 중인 종목 slot을 이 핸들러에 더한다. 엔진이 시작되기 전에만 부른다. */
    public void seedHandedOverSlots(Set<Integer> stockSlots) {
        handedOverSlots.addAll(stockSlots);
    }

    /** 받음이 반영되어 담당 중인 종목 slot의 사본. 복구가 앞 단계에서 만든 목록을 다음 단계의 핸들러로 이어 줄 때 쓴다. 엔진이 시작되기 전에만 부른다. */
    public Set<Integer> acquiredSlotsSnapshot() {
        return Set.copyOf(acquiredSlots);
    }

    /** 복구가 이미 알아낸 담당 중인 종목 slot을 이 핸들러에 더한다. 엔진이 시작되기 전에만 부른다. */
    public void seedAcquiredSlots(Set<Integer> stockSlots) {
        acquiredSlots.addAll(stockSlots);
    }

    /** 배치 끝에서 건드린 종목마다 한 번 알린다. 호가창이 없는 종목은 건너뛰고, listener 예외는 매칭을 멈추지 않게 삼킨다. */
    private void notifyBooksChanged() {
        for (String touched : touchedInBatch) {
            OrderBook book = books.get(touched);
            if (book == null) {
                continue;
            }
            try {
                listener.onBookChanged(touched, book);
            } catch (RuntimeException e) {
                log.log(Level.WARNING, "[매칭] 호가창 변경 알림 실패(매칭은 계속함): 종목=" + touched + " 원인=" + e);
            }
        }
        touchedInBatch.clear();
    }

    /** books를 직렬화해 쓰기 큐에 넣는다(I6 U1). 직렬화(메모리 복사)까지만 이 스레드가 한다. */
    private void takeSnapshot() {
        if (!offerSnapshot()) {
            log.log(Level.WARNING, "[매칭] 스냅샷 쓰기 큐가 가득 차 이번 회차 스킵: appliedSeq=" + appliedSeq);
        }
    }

    private boolean offerSnapshot() {
        MatchingSnapshot snapshot = MatchingSnapshotFactory.capture(books, lastJournaledPosition);
        byte[] snapshotBytes = snapshotCodec.encode(snapshot);
        return snapshotSink.offer(snapshotBytes, lastAppliedOrderIntakePositions(), appliedSeq);
    }

    /**
     * 받은 종목별 호가창 바이트를 호가창에 적재하고(호가창 이동을 받는 쪽, U15), 그 종목 slot의 반영을 다시 켠 뒤 스냅샷을
     * 강제로 찍는다. 스냅샷에 담긴 저널 위치는 이 shard 자신의 값이다(받은 바이트의 위치는 쓰지 않는다). 쓰기 queue가
     * 차서 거부되면 appliedSeq -1로 남겨 재시도 이벤트가 다시 찍게 한다.
     */
    private void handleBookLoad(OrderEvent event) {
        // 그 종목 slot의 기존 호가창을 먼저 비운다. restoreInto는 이미 있는 호가창 위에 주문을 더하므로, 같은 종목 slot을 되받을 때
        // 비우지 않으면 주문이 두 번 쌓인다(11-14절).
        int loadedSlot = event.getStockSlot();
        if (acquiredSlots.contains(loadedSlot)) {
            // 담당이 켜진 뒤에는 이 종목 slot에 이 shard가 접수한 주문과 체결이 있다. 낡은 사본으로 비우고 채우면 그것들이 지워진다.
            log.log(Level.ERROR, "[매칭] 담당 중인 종목 slot에 호가창 적재가 도착해 적용하지 않음: 종목 slot=" + loadedSlot
                + " 이동 번호=" + event.getTransferId());
            appliedSeq++;
            return;
        }
        books.keySet().removeIf(stockCode -> stockSlotHasher.slotFor(stockCode) == loadedSlot);
        for (byte[] bytes : event.getBookBytes()) {
            MatchingSnapshotRestorer.restoreInto(books, snapshotCodec.decode(bytes));
        }
        handedOverSlots.remove(event.getStockSlot());
        appliedSeq++;
        // 적재가 연달아 올 때마다 보드 전체를 인코딩해 스냅샷을 찍으면 종목 slot 수에 비례해 비용이 반복되고 쓰기 큐가 넘친다. 마지막 시도 뒤
        // 간격이 지났으면 지금 찍고(이때까지 보류된 적재 전부가 이 스냅샷으로 확정된다), 아니면 보류만 해 두었다가 재시도 이벤트가 하나로
        // 묶어 찍는다.
        loadsAwaitingDurable.add(new PendingLoad(event.getTransferId(), event.getStockSlot(), -1L));
        if (System.nanoTime() - lastForcedSnapshotAttemptNanos >= FORCE_SNAPSHOT_RETRY_INTERVAL_NANOS) {
            boolean offered = offerSnapshot();
            lastForcedSnapshotAttemptNanos = System.nanoTime();
            if (offered) {
                stampPendingLoads();
            } else {
                log.log(Level.WARNING, "[매칭] 적재 뒤 강제 스냅샷이 거부됨(쓰기 큐 가득): 이동 번호=" + event.getTransferId());
            }
        }
    }

    /** 방금 받아 들여진 스냅샷의 순번을 아직 확정 대기 순번이 없는 적재 전부에 붙인다. */
    private void stampPendingLoads() {
        for (PendingLoad pending : loadsAwaitingDurable) {
            if (pending.appliedSeq < 0) {
                pending.appliedSeq = appliedSeq;
            }
        }
    }

    /**
     * 넘긴 종목 slot의 호가창을 지운다. 넘기는 중 목록({@code handedOverSlots})은 그 종목 slot을 다시 받을 때까지 둔다 — 지운 뒤에
     * 도착한 이벤트가 빈 호가창에 새 호가창을 만들지 못하게 한다.
     */
    private void handleSlotCleanup(OrderEvent event) {
        int cleanedSlot = event.getStockSlot();
        books.keySet().removeIf(stockCode -> stockSlotHasher.slotFor(stockCode) == cleanedSlot);
        slotEventListener.onSlotCleanedUp(cleanedSlot);
    }

    private void retryRejectedForcedSnapshots() {
        if (!hasLoadAwaitingSnapshot()) {
            return;
        }
        appliedSeq++;
        boolean offered = offerSnapshot();
        lastForcedSnapshotAttemptNanos = System.nanoTime();
        if (offered) {
            stampPendingLoads();
        }
    }

    /** 강제 스냅샷이 거부된 적재가 남아 있는지. */
    public boolean hasLoadAwaitingSnapshot() {
        for (PendingLoad pending : loadsAwaitingDurable) {
            if (pending.appliedSeq < 0) {
                return true;
            }
        }
        return false;
    }

    /** 거부된 강제 스냅샷을 다시 찍는 이벤트를 아직 링버퍼에 넣지 않았다면 넣을 권리를 얻는다. */
    public boolean claimForceSnapshotRetry() {
        if (!hasLoadAwaitingSnapshot()) {
            return false;
        }
        // 쓰기 큐가 차 있는 동안 수신 thread의 반복마다 재시도 이벤트를 발행하면 엔진 thread가 매번 스냅샷 전체를 인코딩해 바쁘게 돈다.
        // 마지막 시도(적재 때의 첫 시도나 직전 재시도) 뒤 일정 간격이 지나야 다시 발행한다.
        if (System.nanoTime() - lastForcedSnapshotAttemptNanos < FORCE_SNAPSHOT_RETRY_INTERVAL_NANOS) {
            return false;
        }
        return forceSnapshotRetryQueued.compareAndSet(false, true);
    }

    /**
     * 강제 스냅샷이 디스크에 확정된 적재마다 완료를 알린다({@link MatchingSnapshotSink#durableSeq()}가 그 스냅샷의
     * appliedSeq 이상). 엔진 thread가 아닌 thread에서 부른다.
     *
     * @return 알린 건수
     */
    public int notifyDurableLoads() {
        int notified = 0;
        long durable = snapshotSink.durableSeq();
        for (PendingLoad pending : loadsAwaitingDurable) {
            if (pending.appliedSeq >= 0 && durable >= pending.appliedSeq && loadsAwaitingDurable.remove(pending)) {
                slotEventListener.onBooksLoadedDurable(pending.transferId, pending.stockSlot);
                notified++;
            }
        }
        return notified;
    }

    /**
     * 지금까지 이 소비자가 실제로 반영한 주문의 인테이크 수신 위치를 발행자별로 담은 스냅샷
     * 사본(I2 U1). 아직 처리 전인 주문의 위치는 담기지 않는다 — account-disruptor
     * {@code AccountEventHandler#lastAppliedFillPositions()}와 같은 이유.
     */
    public Map<Integer, Long> lastAppliedOrderIntakePositions() {
        return Map.copyOf(lastAppliedOrderIntakePositions);
    }

    /**
     * 복구 시(단일 스레드, 엔진 start 전) 소비자의 주문 인테이크 수신 위치를 스냅샷이 가리키던
     * 값으로 시드한다 — 안 하면 복구 직후~첫 라이브 주문 사이에 러닝 중 스냅샷이 찍힐 때 위치가
     * 빈 채로 저장돼, 다음 복구가 인테이크 스트림을 처음부터 다시 replay한다(account-disruptor
     * {@code AccountEventHandler#seedLastAppliedFillPositions}와 같은 이유).
     */
    public void seedLastAppliedOrderIntakePositions(Map<Integer, Long> orderIntakePositions) {
        this.lastAppliedOrderIntakePositions.putAll(orderIntakePositions);
    }

    private void handlePlace(OrderEvent event) {
        // 이 이벤트를 실제로 처리하기 시작하는 시점에 위치를 기억한다(account-disruptor
        // AccountEventHandler.handleBuyFill과 같은 자리) — 이후 중복이라 반영을 건너뛰어도
        // "이 위치까지는 이미 살펴봤다"는 사실은 그대로 남아야, 재기동 리플레이가 같은 주문을
        // 다시 들이밀지 않는다.
        lastAppliedOrderIntakePositions.put(event.getSourceSessionId(), event.getSourcePosition());

        OrderBook book = books.computeIfAbsent(event.getStockCode(), k -> new OrderBook());
        if (book.containsOrder(event.getOrderId())) {
            // 멱등성: 같은 주문이 중복 도착하면 무시한다.
            return;
        }
        OrderEntry entry = toEntry(event);
        book.addOrder(entry);
        runMatch(event, book);
    }

    private void handleCancel(OrderEvent event) {
        OrderBook book = books.get(event.getStockCode());
        if (book == null) {
            return;
        }
        book.cancelOrder(event.getOrderId());
    }

    private void runMatch(OrderEvent event, OrderBook book) {
        String stockCode = event.getStockCode();
        boolean firstFill = true;
        while (true) {
            Optional<FillResult> result = book.match();
            if (result.isEmpty()) {
                break;
            }
            fullyFilledOrders.add(book.lastMatchFullyFilledCount());
            if (!stages.isEnabled()) {
                listener.onFill(stockCode, result.get());
                continue;
            }
            // 한 주문이 체결을 여러 건 내면 첫 체결에만 방출 시각과 aggressor 발신 시각을 싣는다(설계서 4-2절).
            long emittedAtNanos = firstFill ? EpochNanos.now() : 0L;
            long aggressorOriginNanos = firstFill ? event.getOriginNanos() : 0L;
            if (firstFill) {
                stages.recordFirstFill(event, emittedAtNanos);
            }
            firstFill = false;
            listener.onFill(stockCode, result.get(), emittedAtNanos, aggressorOriginNanos);
        }
    }

    private static OrderEntry toEntry(OrderEvent event) {
        return new OrderEntry(
            event.getOrderId(),
            event.getAccountId(),
            event.getStockCode(),
            event.getSide(),
            event.getPrice(),
            event.getQuantity(),
            event.getOrderAt()
        );
    }

    /** 전량 체결되어 호가창을 떠난 주문 수의 누적. 어느 thread에서 읽어도 된다. */
    public long fullyFilledOrderCount() {
        return fullyFilledOrders.sum();
    }
}
