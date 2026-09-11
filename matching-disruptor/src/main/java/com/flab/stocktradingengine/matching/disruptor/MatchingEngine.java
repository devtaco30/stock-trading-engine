package com.flab.stocktradingengine.matching.disruptor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadFactory;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

import com.flab.stocktradingengine.codec.JournaledOrder;
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

    private static final MatchListener NO_OP_LISTENER = (stockCode, fill) -> {};

    private final Disruptor<OrderEvent> disruptor;
    private final Journal journal;
    // 종목코드 → 호가창. recover(2c-2)가 새 MatchingEventHandler 인스턴스로 저널을 재적용할 때도
    // 라이브 배선과 같은 맵을 써야 그 위에서 라이브 매칭이 이어진다(account-disruptor AccountEngine
    // 의 accounts 맵과 같은 이유) — 그래서 핸들러가 자체 생성하지 않고 엔진이 필드로 들고 넘긴다.
    private final Map<String, OrderBook> books = new HashMap<>();
    private RingBuffer<OrderEvent> ringBuffer;

    public MatchingEngine(int bufferSize, WaitStrategy waitStrategy, MatchListener listener, Journal journal) {
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
        this.disruptor.handleEventsWith(new JournalEventHandler(journal))
            .then(new MatchingEventHandler(books, listener));
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

    /** 남은 이벤트를 처리하고 소비자 스레드를 종료한다. */
    public void shutdown() {
        disruptor.shutdown();
    }

    /** 저널을 반환한다. 테스트·복구 검증용. */
    public Journal journal() {
        return journal;
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
        MatchingEventHandler recoveryHandler = new MatchingEventHandler(books, NO_OP_LISTENER);
        OrderEvent scratch = new OrderEvent();
        for (JournaledOrder order : entries) {
            applyToScratch(scratch, order);
            recoveryHandler.onEvent(scratch, 0L, false);
        }
    }

    /** 저널 엔트리 하나를 스크래치 슬롯에 채운다 — {@code OrderEvent}의 타입별 setter와 1:1 대응. */
    private void applyToScratch(OrderEvent event, JournaledOrder order) {
        switch (order.type()) {
            case PLACE -> event.setPlace(order.orderId(), order.accountId(), order.stockCode(),
                order.side(), order.price(), order.quantity(), order.orderAt());
            case CANCEL -> event.setCancel(order.orderId(), order.stockCode());
        }
    }

    /** {@link #recover}가 {@link #start} 뒤에 불려 소비자 스레드와 경쟁하는 걸 막는다. */
    private void requireNotStarted() {
        if (ringBuffer != null) {
            throw new IllegalStateException("start() 이후에는 호출할 수 없습니다 — 소비자 스레드와 경쟁합니다");
        }
    }

    /**
     * 주문 접수를 링버퍼에 발행한다.
     *
     * <p>빈 슬롯 자리를 예약({@code next})하고, 그 슬롯에 값을 채운 뒤 발행({@code publish})한다.
     * publish 를 finally 에 두어, 값 채우는 중 예외가 나도 예약한 자리가 막히지 않게 한다.</p>
     */
    public void publishPlace(long orderId, long accountId, String stockCode,
                             OrderSide side, BigDecimal price, int quantity, Instant orderAt) {
        long sequence = ringBuffer.next();
        try {
            OrderEvent event = ringBuffer.get(sequence);
            event.setPlace(orderId, accountId, stockCode, side, price, quantity, orderAt);
        } finally {
            ringBuffer.publish(sequence);
        }
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
