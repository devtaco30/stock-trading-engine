package com.flab.stocktradingengine.matching.disruptor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.ThreadFactory;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

import com.flab.stocktradingengine.trading.entity.OrderSide;

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

    private final Disruptor<OrderEvent> disruptor;
    private final Journal journal;
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
            .then(new MatchingEventHandler(listener));
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
