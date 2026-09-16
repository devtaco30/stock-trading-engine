package com.flab.stocktradingengine.matching.disruptor.handler;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.lmax.disruptor.EventHandler;

import com.flab.stocktradingengine.trading.matching.FillResult;
import com.flab.stocktradingengine.trading.matching.OrderBook;
import com.flab.stocktradingengine.trading.matching.OrderEntry;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.matching.disruptor.engine.OrderEvent;
import com.flab.stocktradingengine.matching.disruptor.io.MatchListener;
import com.flab.stocktradingengine.matching.disruptor.io.MatchingSnapshotSink;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshot;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshotCodec;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshotFactory;

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

    // 종목코드 → 호가창. 단일 스레드만 접근하므로 일반 HashMap 으로 충분하다.
    // MatchingEngine 이 필드로 들고 있다가 넘겨준다 — recover(2c-2)가 새 핸들러 인스턴스로
    // 저널을 재적용할 때도 같은 맵을 공유해야 라이브 매칭이 그 위에서 이어진다
    // (account-disruptor AccountEventHandler 의 accounts 맵과 같은 이유).
    private final Map<String, OrderBook> books;
    private final MatchListener listener;
    private final MatchingSnapshotSink snapshotSink;
    private final boolean snapshotTriggerEnabled;
    private final MatchingSnapshotCodec snapshotCodec = new MatchingSnapshotCodec();

    // JournalEventHandler가 슬롯에 실어준 값(I6 U1) — 이 핸들러(소비자 스레드)만 읽는다.
    private long lastJournaledPosition;
    // 저널 적용 순번(I6 U1) — 처리 결과와 무관하게 이벤트 하나를 소비할 때마다 1씩 증가한다.
    private long appliedSeq;
    // 소비자가 실제로 반영한 주문의 인테이크 수신 위치를 발행자(계좌 샤드 Aeron sessionId)별로
    // 담는다(I2 U1, account-disruptor AccountEventHandler.lastAppliedFillPositions과 같은 이유).
    // 스냅샷 쓰기 스레드가 takeSnapshot() 호출 시점에 이 맵을 읽어가므로(다른 스레드) 동시성 맵.
    private final Map<Integer, Long> lastAppliedOrderIntakePositions = new ConcurrentHashMap<>();

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
        this.books = books;
        this.listener = listener;
        this.snapshotSink = snapshotSink;
        this.snapshotTriggerEnabled = snapshotTriggerEnabled;
    }

    @Override
    public void onEvent(OrderEvent event, long sequence, boolean endOfBatch) {
        lastJournaledPosition = event.getJournaledPosition();
        try {
            if (event.getType() == EventType.PLACE) {
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
        appliedSeq++;
        if (snapshotTriggerEnabled && appliedSeq % SNAPSHOT_INTERVAL_JOURNAL_ENTRIES == 0) {
            takeSnapshot();
        }
    }

    /** books를 직렬화해 쓰기 큐에 넣는다(I6 U1). 직렬화(메모리 복사)까지만 이 스레드가 한다. */
    private void takeSnapshot() {
        MatchingSnapshot snapshot = MatchingSnapshotFactory.capture(books, lastJournaledPosition);
        byte[] snapshotBytes = snapshotCodec.encode(snapshot);
        boolean offered = snapshotSink.offer(snapshotBytes, lastAppliedOrderIntakePositions(), appliedSeq);
        if (!offered) {
            log.log(Level.WARNING, "[매칭] 스냅샷 쓰기 큐가 가득 차 이번 회차 스킵: appliedSeq=" + appliedSeq);
        }
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
        runMatch(event.getStockCode(), book);
    }

    private void handleCancel(OrderEvent event) {
        OrderBook book = books.get(event.getStockCode());
        if (book == null) {
            return;
        }
        book.cancelOrder(event.getOrderId());
    }

    private void runMatch(String stockCode, OrderBook book) {
        while (true) {
            Optional<FillResult> result = book.match();
            if (result.isEmpty()) {
                break;
            }
            listener.onFill(stockCode, result.get());
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
}
