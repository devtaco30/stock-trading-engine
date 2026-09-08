package com.flab.stocktradingengine.matching.disruptor;

import java.util.concurrent.atomic.AtomicBoolean;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;

import com.flab.stocktradingengine.wire.EventType;
import com.flab.stocktradingengine.wire.JournaledOrder;
import com.flab.stocktradingengine.wire.OrderCodec;

/**
 * Unit 4c — Aeron 으로 들어온 주문을 매칭엔진에 넣는 수신 게이트웨이.
 *
 * <p>인메모리 피더가 하던 일을 대체한다. 전용 스레드가 Aeron {@link Subscription} 을 계속 폴링해,
 * 도착한 바이트를 {@link OrderCodec} 로 디코딩하고 {@link MatchingEngine} 의 발행 메서드
 * ({@code publishPlace}/{@code publishCancel})로 링버퍼에 넣는다. 여기까지가 전달 계층이고,
 * 그 뒤 저널·매칭은 기존 파이프라인이 그대로 처리한다.</p>
 *
 * <h3>왜 폴링 스레드인가</h3>
 * <p>Aeron 은 수신자가 직접 {@code subscription.poll(...)} 를 호출해야 데이터를 가져온다(콜백을 알아서
 * 불러주지 않는다). 그래서 이 스레드가 반복 폴링한다. 새 메시지가 없을 때 CPU 를 어떻게 쓸지는
 * {@link IdleStrategy}(유휴 전략)가 정한다 — Disruptor 의 WaitStrategy 와 같은 자리의 개념이다.
 * 여기선 {@link BackoffIdleStrategy}(놀면 점점 더 물러나 대기)를 쓴다.</p>
 *
 * <h3>단일 프로듀서 유지</h3>
 * <p>이 폴링 스레드 하나만 {@code publishPlace} 를 호출하므로, {@link MatchingEngine} 의
 * {@code ProducerType.SINGLE} 전제가 그대로 지켜진다.</p>
 */
public final class AeronOrderReceiver implements AutoCloseable {

    private static final int FRAGMENT_LIMIT = 10;
    private static final long CLOSE_JOIN_TIMEOUT_MILLIS = 1000L;

    private final Subscription subscription;
    private final MatchingEngine engine;
    private final OrderCodec codec = new OrderCodec();
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final FragmentHandler fragmentHandler = this::onFragment;

    private Thread pollThread;

    public AeronOrderReceiver(Subscription subscription, MatchingEngine engine) {
        this.subscription = subscription;
        this.engine = engine;
    }

    /** 폴링 스레드를 기동한다. */
    public void start() {
        running.set(true);
        pollThread = new Thread(this::pollLoop, "aeron-order-receiver");
        pollThread.setDaemon(true);
        pollThread.start();
    }

    private void pollLoop() {
        while (running.get()) {
            int fragments = subscription.poll(fragmentHandler, FRAGMENT_LIMIT);
            idleStrategy.idle(fragments);
        }
    }

    private void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        JournaledOrder order = codec.decode(buffer, offset);
        if (order.type() == EventType.PLACE) {
            engine.publishPlace(
                order.orderId(), order.accountId(), order.stockCode(),
                order.side(), order.price(), order.quantity(), order.orderAt());
        } else {
            engine.publishCancel(order.orderId(), order.stockCode());
        }
    }

    /** 폴링 스레드를 멈추고 종료를 기다린다. */
    @Override
    public void close() {
        running.set(false);
        if (pollThread != null) {
            try {
                pollThread.join(CLOSE_JOIN_TIMEOUT_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
