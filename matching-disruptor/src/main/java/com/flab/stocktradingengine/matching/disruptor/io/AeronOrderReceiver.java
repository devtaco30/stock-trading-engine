package com.flab.stocktradingengine.matching.disruptor.io;

import java.util.concurrent.atomic.AtomicBoolean;

import java.util.function.IntPredicate;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;

import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;

import com.flab.stocktradingengine.aeron.StockSlotHasher;
import com.flab.stocktradingengine.support.IdleStrategies;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.time.EpochNanos;
import com.flab.stocktradingengine.codec.OrderCodec;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;

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
 * <h3>담당 확인</h3>
 * <p>PLACE는 종목코드를 종목 slot으로 바꿔 담당 판정({@code ownedSlots})을 통과한 것만
 * {@code publishPlace}로 넣는다. 담당이 아니면 접수하지 않고 {@link NotOwnedOrderListener}에 넘긴다.
 * 예외를 던지지 않으므로 폴링 thread가 죽지 않는다. 담당 판정과 hasher를 주지 않는 생성자는 모든
 * 주문을 담당으로 본다(샤딩을 모르는 기존 구성).</p>
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
    private final IdleStrategy idleStrategy;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final FragmentHandler fragmentHandler = this::onFragment;
    private final IntPredicate ownedSlots;
    private final StockSlotHasher stockSlotHasher;
    private final NotOwnedOrderListener notOwnedOrderListener;
    private final SlotEventRequests slotEventRequests;

    private Thread pollThread;

    public AeronOrderReceiver(Subscription subscription, MatchingEngine engine) {
        this(subscription, engine, IdleStrategies.forThread());
    }

    /** 쉬는 방식 변경 실험(EXPERIMENT_IDLE)이 폴링 스레드의 쉬는 방식을 바꿀 수 있게 전략을 주입받는다. */
    public AeronOrderReceiver(Subscription subscription, MatchingEngine engine, IdleStrategy idleStrategy) {
        this(subscription, engine, idleStrategy, slot -> true, new StockSlotHasher(1), NotOwnedOrderListener.NO_OP);
    }

    public AeronOrderReceiver(Subscription subscription, MatchingEngine engine,
                              IntPredicate ownedSlots, StockSlotHasher stockSlotHasher,
                              NotOwnedOrderListener notOwnedOrderListener) {
        this(subscription, engine, IdleStrategies.forThread(), ownedSlots, stockSlotHasher, notOwnedOrderListener);
    }

    public AeronOrderReceiver(Subscription subscription, MatchingEngine engine, IdleStrategy idleStrategy,
                              IntPredicate ownedSlots, StockSlotHasher stockSlotHasher,
                              NotOwnedOrderListener notOwnedOrderListener) {
        this(subscription, engine, idleStrategy, ownedSlots, stockSlotHasher, notOwnedOrderListener,
            new SlotEventRequests());
    }

    /** 종목 slot 받음·넘김 요청 통로를 함께 받는다. 이 thread가 주문과 함께 그 요청을 이벤트로 발행한다. */
    public AeronOrderReceiver(Subscription subscription, MatchingEngine engine, IdleStrategy idleStrategy,
                              IntPredicate ownedSlots, StockSlotHasher stockSlotHasher,
                              NotOwnedOrderListener notOwnedOrderListener, SlotEventRequests slotEventRequests) {
        this.slotEventRequests = slotEventRequests;
        this.subscription = subscription;
        this.engine = engine;
        this.idleStrategy = idleStrategy;
        this.ownedSlots = ownedSlots;
        this.stockSlotHasher = stockSlotHasher;
        this.notOwnedOrderListener = notOwnedOrderListener;
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
            int slotEvents = drainSlotEvents();
            int fragments = subscription.poll(fragmentHandler, FRAGMENT_LIMIT);
            idleStrategy.idle(fragments + slotEvents);
        }
    }

    /**
     * 패키지 가시성 — 단위 테스트가 실제 Subscription 없이 이 메서드를 직접 호출한다.
     *
     * <p>{@code header.position()}(I2 U1)은 "이 메시지를 읽은 뒤 image가 도달한 위치"다 — 소비자
     * (매칭 엔진)가 이 주문을 실제로 반영한 뒤 발행자별 위치 맵으로 기억해, 아직 링에만 들어가고
     * 처리는 안 된 주문의 위치와 구분한다. {@code header.sessionId()}는 이 위치가 어느
     * 발행자(Aeron 연결, 계좌 샤드마다 다르다)의 것인지 구분하는 키다(account-disruptor
     * {@code AccountFillReceiver}와 같은 이유). 취소는 아직 Aeron으로 오지 않아(계좌 쪽에
     * forwardCancel이 없다) sourcePosition·sourceSessionId를 붙이지 않는다.</p>
     */
    void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        long matchReceivedAtNanos = engine.stagesEnabled() ? EpochNanos.now() : 0L;
        JournaledOrder order = codec.decode(buffer, offset);
        if (order.type() == EventType.PLACE) {
            long sourcePosition = header != null ? header.position() : 0L;
            int sourceSessionId = header != null ? header.sessionId() : 0;
            int stockSlot = stockSlotHasher.slotFor(order.stockCode());
            if (!ownedSlots.test(stockSlot)) {
                notOwnedOrderListener.onNotOwned(order);
                // 접수하지 않은 주문도 이 발행자의 위치는 앞으로 나아가야 한다(11-4절 3항).
                engine.publishPositionOnly(sourceSessionId, sourcePosition);
                return;
            }
            engine.publishPlace(
                order.orderId(), order.accountId(), order.stockCode(),
                order.side(), order.price(), order.quantity(), order.orderAt(),
                sourcePosition, sourceSessionId,
                order.originNanos(), order.receivedAtNanos(), order.acceptedAtNanos(), matchReceivedAtNanos);
        } else if (order.type() == EventType.CANCEL) {
            engine.publishCancel(order.orderId(), order.stockCode());
        }
        // 종목 slot 받음·넘김은 저널에만 기록되는 항목이라 인테이크 와이어로 받아도 엔진에 넣지 않는다.
    }

    /** 쌓인 종목 slot 받음·넘김 요청을 이벤트로 발행한다. 패키지 가시성 — 단위 테스트가 Subscription 없이 부른다. */
    int drainSlotEvents() {
        return slotEventRequests.drainTo(engine);
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
