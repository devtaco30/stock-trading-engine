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
 * 내부에 소비자로 {@link MatchingEventHandler} 하나를 붙여 단일 스레드 매칭을 구성한다.</p>
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
    private RingBuffer<OrderEvent> ringBuffer;

    public MatchingEngine(int bufferSize, WaitStrategy waitStrategy, MatchListener listener) {
        ThreadFactory threadFactory = DaemonThreadFactory.INSTANCE;
        this.disruptor = new Disruptor<>(
            OrderEvent::new,
            bufferSize,
            threadFactory,
            ProducerType.SINGLE,
            waitStrategy
        );
        this.disruptor.handleEventsWith(new MatchingEventHandler(listener));
    }

    /** 기본 대기 전략({@link BlockingWaitStrategy})으로 생성한다. */
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
