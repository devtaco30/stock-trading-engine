package com.flab.stocktradingengine.matching.disruptor.engine;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

import com.flab.stocktradingengine.aeron.StockSlotHasher;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.matching.disruptor.handler.JournalEventHandler;
import com.flab.stocktradingengine.matching.disruptor.handler.MatchingEventHandler;
import com.flab.stocktradingengine.matching.disruptor.handler.MatchingExceptionHandler;
import com.flab.stocktradingengine.matching.disruptor.io.AcquireConfirmations;
import com.flab.stocktradingengine.matching.disruptor.io.MatchListener;
import com.flab.stocktradingengine.matching.disruptor.io.MatchingSnapshotSink;
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventListener;
import com.flab.stocktradingengine.matching.disruptor.journal.InMemoryJournal;
import com.flab.stocktradingengine.matching.disruptor.journal.Journal;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshot;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshotFactory;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshotRestorer;
import com.flab.stocktradingengine.time.EpochNanos;
import com.flab.stocktradingengine.time.StageLatencyRegistry;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.matching.OrderBook;

/**
 * Disruptor 매칭 코어의 진입점.
 *
 * <p>링버퍼 생명주기(start/shutdown)와 주문 발행(프로듀서)을 한곳에 모은다.
 * 소비자를 {@code handleEventsWith(journal).then(matcher)} 로 배선해,
 * {@link JournalEventHandler}(기록)가 먼저 돌고 그 뒤에만 {@link MatchingEventHandler}(매칭)가 돈다.
 * 매칭은 단일 스레드로 처리된다.</p>
 *
 * <h3>ProducerType.SINGLE</h3>
 * <p>Unit 1 에서는 주문을 넣는 피더 스레드가 하나다. 프로듀서가 하나임을 알려주면
 * 링버퍼가 프로듀서 간 경쟁을 처리하는 비용을 줄인다. 여러 피더가 필요해지면 MULTI 로 바꾼다.</p>
 *
 * <h3>WaitStrategy</h3>
 * <p>소비자가 새 이벤트를 기다리는 방식을 생성자로 주입한다. 기본은
 * {@link BlockingWaitStrategy}(조건 변수로 대기, CPU 를 태우지 않음)다.
 * 대기 전략별 처리량 비교는 이후 단위에서 다룬다.</p>
 */
public class MatchingEngine {

    // 종목 slot 개수의 기본값. matching-worker의 matching-shard.stock-slot-count 기본값과 같다.
    private static final int DEFAULT_STOCK_SLOT_COUNT = 64;

    private static final MatchListener NO_OP_LISTENER = (stockCode, fill) -> {};

    private static final MatchingSnapshotSink NO_OP_SNAPSHOT_SINK = new MatchingSnapshotSink() {
        @Override
        public boolean offer(byte[] snapshotBytes, Map<Integer, Long> orderIntakePositions, long appliedSeq) {
            return true;
        }

        @Override
        public long durableSeq() {
            return 0L;
        }
    };

    // 소비자 스레드가 fail-fast로 죽으면 disruptor.shutdown()이 hasBacklog() 스핀에서 무한 대기할 수
    // 있다 — 죽은 소비자 뒤에 물린 핸들러가 그 시퀀스를 영원히 기다리며 블록되기 때문. 이 타임아웃
    // 안에 못 끝나면 halt()로 강제 정지한다.
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;

    private final Disruptor<OrderEvent> disruptor;
    private final Journal journal;
    // 종목코드 → 호가창. recover(2c-2)가 새 MatchingEventHandler 인스턴스로 저널을 재적용할 때도
    // 라이브 배선과 같은 맵을 써야 그 위에서 라이브 매칭이 이어진다(account-disruptor AccountEngine
    // 의 accounts 맵과 같은 이유) — 그래서 핸들러가 자체 생성하지 않고 엔진이 필드로 들고 넘긴다.
    private final Map<String, OrderBook> books = new HashMap<>();
    // 라이브 배선에 쓴 것과 같은 인스턴스를 들고 있어야 lastAppliedOrderIntakePositions()가 실제
    // 소비자 스레드가 반영한 값을 읽는다(account-disruptor AccountEngine.businessHandler와 같은 이유).
    private final MatchingEventHandler matchingEventHandler;
    private final StockSlotHasher stockSlotHasher;
    private final SlotEventListener slotEventListener;
    private final AcquireConfirmations acquireConfirmations;
    private RingBuffer<OrderEvent> ringBuffer;
    private final MatchingSegmentStages stages;

    /**
     * @param snapshotSink 러닝 중 스냅샷을 내보낼 싱크(I6 U1~U2). {@link #snapshot()}과 달리
     *     소비자 스레드가 N건마다 직접 부른다 — 실제 파일 쓰기는 matching-worker의 전용 쓰기
     *     스레드(구현체)가 맡는다(ADR-024).
     * @param snapshotTriggerEnabled N건마다 스냅샷을 찍을지. recover()의 replay 전용 핸들러는
     *     항상 false로 둔다(이미 지나간 저널을 다시 훑는 것뿐이라 새로 찍을 스냅샷이 없다).
     */
    public MatchingEngine(int bufferSize, WaitStrategy waitStrategy, MatchListener listener, Journal journal,
                          MatchingSnapshotSink snapshotSink, boolean snapshotTriggerEnabled) {
        this(bufferSize, waitStrategy, listener, journal, snapshotSink, snapshotTriggerEnabled, new StageLatencyRegistry(false));
    }

    /** 구간 분해 측정용 — 단계별 지연 등록부를 받는다(꺼진 등록부면 기존 동작과 같다). */
    public MatchingEngine(int bufferSize, WaitStrategy waitStrategy, MatchListener listener, Journal journal,
                          MatchingSnapshotSink snapshotSink, boolean snapshotTriggerEnabled, StageLatencyRegistry stageRegistry) {
        this(bufferSize, waitStrategy, listener, journal, snapshotSink, snapshotTriggerEnabled, stageRegistry,
            new StockSlotHasher(DEFAULT_STOCK_SLOT_COUNT), SlotEventListener.NO_OP);
    }

    /** 종목 slot 받음·넘김 이벤트(U13)를 처리하는 통지와 종목코드→종목 slot 변환기를 함께 받는다. */
    public MatchingEngine(int bufferSize, WaitStrategy waitStrategy, MatchListener listener, Journal journal,
                          StockSlotHasher stockSlotHasher, SlotEventListener slotEventListener) {
        this(bufferSize, waitStrategy, listener, journal, NO_OP_SNAPSHOT_SINK, false, new StageLatencyRegistry(false),
            stockSlotHasher, slotEventListener);
    }

    public MatchingEngine(int bufferSize, WaitStrategy waitStrategy, MatchListener listener, Journal journal,
                          MatchingSnapshotSink snapshotSink, boolean snapshotTriggerEnabled, StageLatencyRegistry stageRegistry,
                          StockSlotHasher stockSlotHasher, SlotEventListener slotEventListener) {
        this(bufferSize, waitStrategy, listener, journal, snapshotSink, snapshotTriggerEnabled, stageRegistry,
            stockSlotHasher, slotEventListener, null);
    }

    /**
     * @param archiveRecordedPosition Archive가 저널 녹화에 지금까지 기록한 위치. 주면 받음 항목의 담당 켜기를 Archive 기록 위치가
     *     항목 끝 위치에 닿은 뒤로 미룬다({@link #pollLoadCompletions}를 부르는 thread가 확인한다). null이면 append 직후 바로 켠다.
     */
    public MatchingEngine(int bufferSize, WaitStrategy waitStrategy, MatchListener listener, Journal journal,
                          MatchingSnapshotSink snapshotSink, boolean snapshotTriggerEnabled, StageLatencyRegistry stageRegistry,
                          StockSlotHasher stockSlotHasher, SlotEventListener slotEventListener,
                          LongSupplier archiveRecordedPosition) {
        this.slotEventListener = slotEventListener;
        this.acquireConfirmations = archiveRecordedPosition == null ? null : new AcquireConfirmations(archiveRecordedPosition);
        this.stages = new MatchingSegmentStages(stageRegistry);
        this.stockSlotHasher = stockSlotHasher;
        this.journal = journal;
        ThreadFactory threadFactory = DaemonThreadFactory.INSTANCE;
        this.disruptor = new Disruptor<>(
            OrderEvent::new,
            bufferSize,
            threadFactory,
            ProducerType.SINGLE,
            waitStrategy
        );
        // 예상 못한 예외(NPE 등 버그·상태 오염)는 fail-fast 로 소비자를 멈춘다.
        // handleEventsWith 배선 전에 설정해야 이후 등록되는 모든 핸들러에 적용된다.
        this.disruptor.setDefaultExceptionHandler(new MatchingExceptionHandler());
        // 저널러가 먼저 기록 → 매처가 그 뒤에 매칭 (SequenceBarrier 로 게이팅)
        this.matchingEventHandler = new MatchingEventHandler(books, listener, snapshotSink, snapshotTriggerEnabled, stages,
            stockSlotHasher, slotEventListener);
        this.disruptor.handleEventsWith(new JournalEventHandler(journal, slotEventListener, acquireConfirmations)).then(matchingEventHandler);
    }

    /** 단계 측정 등록부를 받고 러닝 중 스냅샷 없이 생성한다 — 테스트·측정용. */
    public MatchingEngine(int bufferSize, WaitStrategy waitStrategy, MatchListener listener, Journal journal,
                          StageLatencyRegistry stageRegistry) {
        this(bufferSize, waitStrategy, listener, journal, NO_OP_SNAPSHOT_SINK, false, stageRegistry);
    }

    /** 러닝 중 스냅샷 없이(트리거 꺼짐) 생성한다 — 테스트·recover() 전용 핸들러가 쓰는 편의 생성자. */
    public MatchingEngine(int bufferSize, WaitStrategy waitStrategy, MatchListener listener, Journal journal) {
        this(bufferSize, waitStrategy, listener, journal, NO_OP_SNAPSHOT_SINK, false);
    }

    /** 기본 저널({@link InMemoryJournal})로 생성한다. */
    public MatchingEngine(int bufferSize, WaitStrategy waitStrategy, MatchListener listener) {
        this(bufferSize, waitStrategy, listener, new InMemoryJournal());
    }

    /** 기본 대기 전략({@link BlockingWaitStrategy}) + 기본 저널({@link InMemoryJournal})로 생성한다. */
    public MatchingEngine(int bufferSize, MatchListener listener) {
        this(bufferSize, new BlockingWaitStrategy(), listener);
    }

    /** 소비자 스레드를 기동하고 링버퍼를 준비한다. */
    public void start() {
        this.ringBuffer = disruptor.start();
    }

    /**
     * 남은 이벤트를 처리하고 소비자 스레드를 종료한다. 죽은 소비자 앞에서 무한 대기하지 않도록
     * {@link #SHUTDOWN_TIMEOUT_SECONDS} 안에 못 끝나면 강제로 halt한다.
     */
    public void shutdown() {
        try {
            disruptor.shutdown(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            disruptor.halt();
        }
    }

    /** 저널을 반환한다. 테스트·복구 검증용. */
    public Journal journal() {
        return journal;
    }

    /**
     * 지금까지 소비자가 실제로 반영한 주문의 인테이크 수신 위치를 발행자(계좌 샤드 Aeron
     * sessionId)별로 담은 맵(I2 U1). account-disruptor {@link
     * com.flab.stocktradingengine.account.disruptor.engine.AccountEngine#lastAppliedFillPositions}과
     * 같은 이유 — 러닝 중 스냅샷·graceful stop 스냅샷 둘 다 이 값을 파일에 남겨야, 재기동
     * 리플레이가 이미 반영한 주문을 다시 들이밀지 않는다.
     */
    public Map<Integer, Long> lastAppliedOrderIntakePositions() {
        return matchingEventHandler.lastAppliedOrderIntakePositions();
    }

    /**
     * 복구 시(단일 스레드, {@link #start} 전) 소비자의 주문 인테이크 수신 위치를 스냅샷이 가리키던
     * 값으로 시드한다 — 반드시 {@link #restore} 이후, {@link #start} 이전에 부른다.
     */
    public void seedOrderIntakePositions(Map<Integer, Long> orderIntakePositions) {
        requireNotStarted();
        matchingEventHandler.seedLastAppliedOrderIntakePositions(orderIntakePositions);
    }

    /** 종목의 호가창에 해당 주문이 미체결로 남아있는지 확인한다. 테스트·복구 검증용 — {@link #journal()}과 같은 자리. */
    public boolean containsOrder(String stockCode, long orderId) {
        OrderBook book = books.get(stockCode);
        return book != null && book.containsOrder(orderId);
    }

    /**
     * 저널 엔트리를 순서대로 재적용해 호가창을 되살린다(2c-2). 반드시 {@link #start} 전에(설정
     * 스레드에서만) 호출한다 — 기동 후엔 소비자 스레드와 경쟁한다.
     *
     * <p>실제 매칭 로직({@link MatchingEventHandler})을 그대로 재사용해 호가창을 똑같이
     * 재구성하되, 체결 통지는 no-op으로 막는다 — 재시작 전 체결은 이미 계좌 축(Kafka)이 durable
     * 하게 받았으므로 다시 내보내면 중복 체결 발행이 된다. 링버퍼·저널도 거치지 않는다(이미
     * 기록된 입력을 다시 저널에 넣거나 링에 발행할 이유가 없다).</p>
     */
    public void recover(Iterable<JournaledOrder> entries) {
        requireNotStarted();
        // 넘김 항목도 살아 있는 핸들러와 같게 다룬다. 넘긴 뒤 같은 종목 slot에 온 이벤트는 반영하지 않고, 그 목록을 복구가 끝난
        // 뒤의 살아 있는 핸들러와 다음 복구 단계로 이어 준다.
        MatchingEventHandler recoveryHandler = new MatchingEventHandler(books, NO_OP_LISTENER, NO_OP_SNAPSHOT_SINK, false,
            MatchingSegmentStages.DISABLED, stockSlotHasher, SlotEventListener.NO_OP);
        recoveryHandler.seedHandedOverSlots(matchingEventHandler.handedOverSlotsSnapshot());
        OrderEvent scratch = new OrderEvent();
        for (JournaledOrder order : entries) {
            applyToScratch(scratch, order);
            recoveryHandler.onEvent(scratch, 0L, false);
        }
        matchingEventHandler.seedHandedOverSlots(recoveryHandler.handedOverSlotsSnapshot());
    }

    /**
     * 현재 호가창·전량 체결 멱등 캐시·저널 위치를 통째로 찍는다(2d-1, ADR-019 "자체 스냅샷").
     *
     * <p>graceful shutdown(Disruptor drain 완료, 소비자 스레드 quiescent) 시점에만 안전하다 —
     * 소비자 스레드가 아직 books 를 만지고 있는 도중에 부르면 단일 스레드 전제가 깨진 채로 읽게
     * 된다. 언제가 안전한지 판단하는 건 호출부(matching-worker 호스트) 책임이다.</p>
     */
    public MatchingSnapshot snapshot() {
        return MatchingSnapshotFactory.capture(books, journal.position());
    }

    /**
     * 스냅샷으로 호가창·멱등 캐시를 되살린다(2d-1a). 반드시 {@link #start} 전에 호출한다.
     * 저널을 스냅샷 위치부터 리플레이해 그 뒤의 변화까지 마저 채우는 건 호출부(matching-worker
     * 호스트, 2d-1b) 책임이다 — 이 메서드는 스냅샷 자체만 되살린다.
     */
    public void restore(MatchingSnapshot snapshot) {
        requireNotStarted();
        MatchingSnapshotRestorer.restoreInto(books, snapshot);
    }

    /** 저널 엔트리 하나를 스크래치 슬롯에 채운다 — {@code OrderEvent}의 타입별 setter와 1:1 대응. */
    private void applyToScratch(OrderEvent event, JournaledOrder order) {
        switch (order.type()) {
            case PLACE -> event.setPlace(order.orderId(), order.accountId(), order.stockCode(),
                order.side(), order.price(), order.quantity(), order.orderAt());
            case CANCEL -> event.setCancel(order.orderId(), order.stockCode());
            case SLOT_ACQUIRED, SLOT_RELEASED -> event.setSlotEvent(order.type(), order.stockSlot());
        }
    }

    /** {@link #recover}가 {@link #start} 뒤에 불려 소비자 스레드와 경쟁하는 걸 막는다. */
    private void requireNotStarted() {
        if (ringBuffer != null) {
            throw new IllegalStateException("start() 이후에는 호출할 수 없습니다 — 소비자 스레드와 경쟁합니다");
        }
    }

    /**
     * 주문 접수를 링버퍼에 발행한다. sourcePosition·sourceSessionId 를 모르는 발행자(테스트,
     * 저널 replay 등)를 위한 오버로드 — 둘 다 0으로 채운다.
     *
     * <p>빈 슬롯 자리를 예약({@code next})하고, 그 슬롯에 값을 채운 뒤 발행({@code publish})한다.
     * publish 를 finally 에 두어, 값 채우는 중 예외가 나도 예약한 자리가 막히지 않게 한다.</p>
     */
    public void publishPlace(long orderId, long accountId, String stockCode,
                             OrderSide side, BigDecimal price, int quantity, Instant orderAt) {
        publishPlace(orderId, accountId, stockCode, side, price, quantity, orderAt, 0L, 0);
    }

    /**
     * 주문 접수를 링버퍼에 발행한다. sourcePosition 은 이 주문을 실어 보낸 인테이크 수신 스트림의
     * 위치(I2 U1, {@code AeronOrderReceiver}가 {@code Header.position()}으로 실어 보낸다).
     * sourceSessionId 는 그 위치가 어느 발행자(Aeron sessionId=계좌 샤드)의 것인지 구분하는 키
     * ({@code Header.sessionId()}).
     */
    public void publishPlace(long orderId, long accountId, String stockCode,
                             OrderSide side, BigDecimal price, int quantity, Instant orderAt,
                             long sourcePosition, int sourceSessionId) {
        publishPlace(orderId, accountId, stockCode, side, price, quantity, orderAt, sourcePosition, sourceSessionId,
            0L, 0L, 0L, 0L);
    }

    /**
     * 구간 분해 측정용 — 계좌에서 실려 온 세 시각(발신 W-P0, 수신 W-P2, 승인 후 전달 W-P8)과 매칭 수신
     * 폴링 스레드가 읽은 시각(W-P11)을 함께 싣는다. 계측이 꺼져 있으면 시각은 무시된다.
     */
    public void publishPlace(long orderId, long accountId, String stockCode,
                             OrderSide side, BigDecimal price, int quantity, Instant orderAt,
                             long sourcePosition, int sourceSessionId,
                             long originNanos, long receivedAtNanos, long acceptedAtNanos, long matchReceivedAtNanos) {
        long sequence = ringBuffer.next();
        try {
            OrderEvent event = ringBuffer.get(sequence);
            event.setPlace(orderId, accountId, stockCode, side, price, quantity, orderAt, sourcePosition, sourceSessionId);
            if (stages.isEnabled()) {
                event.setSegmentTimes(originNanos, receivedAtNanos, acceptedAtNanos, matchReceivedAtNanos);
                event.setRingClaimedAtNanos(EpochNanos.now());
            }
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    /**
     * 담당이 아니라 접수하지 않은 주문의 위치만 링버퍼에 발행한다(11-4절 3항). 엔진 thread가 이 이벤트로
     * 발행자별 반영 위치만 앞으로 나아가게 한다. 호가창은 건드리지 않고 저널에는 기록하지 않는다.
     * 발행자는 주문 수신 폴링 thread 하나뿐이라 단일 프로듀서 전제가 유지된다.
     */
    public void publishPositionOnly(int sourceSessionId, long sourcePosition) {
        long sequence = ringBuffer.next();
        try {
            OrderEvent event = ringBuffer.get(sequence);
            event.setPositionOnly(sourcePosition, sourceSessionId);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    /**
     * 종목 slot을 받았다는 이벤트를 링버퍼에 발행한다. 주문과 같은 프로듀서 thread에서만 불러야 한다
     * ({@code ProducerType.SINGLE}). 저널에 기록되면 {@link SlotEventListener#onSlotAcquiredJournaled}가 불린다.
     */
    public void publishSlotAcquired(int stockSlot) {
        publishSlotEvent(EventType.SLOT_ACQUIRED, stockSlot);
    }

    /**
     * 종목 slot을 넘긴다는 이벤트를 링버퍼에 발행한다. 매칭 핸들러가 이 이벤트에 도달하면 그 종목 slot의 호가창 바이트를
     * {@link SlotEventListener#onSlotHandedOver}로 내보내고 이후 그 종목 slot의 주문을 반영하지 않는다.
     */
    public void publishSlotReleased(int stockSlot) {
        publishSlotEvent(EventType.SLOT_RELEASED, stockSlot);
    }

    /**
     * 호가창 적재 이벤트를 링버퍼에 발행한다(U15). 주문과 같은 프로듀서 thread에서만 불러야 한다. 엔진 thread가
     * 종목별 바이트를 호가창에 적재하고 스냅샷을 강제로 찍는다. 디스크에 확정되면
     * {@link SlotEventListener#onBooksLoadedDurable}가 {@link #pollLoadCompletions}를 부른 thread에서 불린다.
     */
    public void publishBookLoad(long transferId, int stockSlot, List<byte[]> bookBytesPerStock) {
        long sequence = ringBuffer.next();
        try {
            OrderEvent event = ringBuffer.get(sequence);
            event.setBookLoad(transferId, stockSlot, bookBytesPerStock);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    /**
     * 적재 뒤 스냅샷이 디스크에 확정되었는지 확인해 완료를 알리고, 쓰기 queue가 차서 거부된 강제 스냅샷이 남아 있으면
     * 재시도 이벤트를 발행한다. 주문을 발행하는 프로듀서 thread의 반복 안에서 부른다.
     *
     * @return 알리거나 재시도를 발행한 건수
     */
    public int pollLoadCompletions() {
        int handled = matchingEventHandler.notifyDurableLoads();
        if (acquireConfirmations != null) {
            handled += acquireConfirmations.confirmRecorded(slotEventListener);
        }
        if (matchingEventHandler.claimForceSnapshotRetry()) {
            long sequence = ringBuffer.next();
            try {
                ringBuffer.get(sequence).setForceSnapshot();
            } finally {
                ringBuffer.publish(sequence);
            }
            handled++;
        }
        return handled;
    }

    private void publishSlotEvent(EventType slotEventType, int stockSlot) {
        long sequence = ringBuffer.next();
        try {
            OrderEvent event = ringBuffer.get(sequence);
            event.setSlotEvent(slotEventType, stockSlot);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    /** 수신 폴링 스레드가 W-P11을 찍을지 정하는 데 쓴다. */
    public boolean stagesEnabled() {
        return stages.isEnabled();
    }

    /** 주문 취소를 링버퍼에 발행한다. */
    public void publishCancel(long orderId, String stockCode) {
        long sequence = ringBuffer.next();
        try {
            OrderEvent event = ringBuffer.get(sequence);
            event.setCancel(orderId, stockCode);
        } finally {
            ringBuffer.publish(sequence);
        }
    }
}
