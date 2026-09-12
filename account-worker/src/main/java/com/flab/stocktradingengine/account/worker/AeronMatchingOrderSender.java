package com.flab.stocktradingengine.account.worker;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;

import com.flab.stocktradingengine.account.disruptor.MatchingOrderSender;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.codec.OrderCodec;
import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.Publication;

import lombok.extern.slf4j.Slf4j;

/**
 * {@link MatchingOrderSender}의 Aeron 구현 — 계좌가 accept한 매수·매도를 매칭 인테이크
 * (파이프라인 연결 ①, stream {@link MatchingOrderSenderConfig#MATCHING_STREAM_ID})로 인코딩해 보낸다.
 *
 * <h3>발신을 로직 스레드 밖으로 (LMAX 1단계)</h3>
 * <p>{@link #forwardPlace}는 계좌 엔진의 단일 상시 컨슈머 스레드에서 호출된다 — 그 스레드가 직접
 * Aeron {@code offer}(네트워크 I/O)를 하면, single-writer가 I/O로 블로킹·중단될 수 있다(LMAX 원칙
 * 위반). 그래서 {@code forwardPlace}는 {@link #outbox}(SPSC 락프리 큐, 프로듀서=계좌 스레드 하나·
 * 컨슈머=전용 {@link #publisherThread} 하나)에 {@code offer} 논블로킹 1회로 넣기만 하고 즉시 돌아간다.
 * 실제 인코딩·Aeron 발신은 {@link #publisherThread}가 큐를 드레인하며 한다 — 그 스레드는 전용이라
 * 블로킹 재시도해도 계좌 로직을 막지 않는다.</p>
 *
 * <p>큐가 가득 차서 {@code offer}가 실패하면(publisher가 계속 밀리는 상황) 로그만 남기고 그 주문은
 * 매칭에 발신되지 않는다(계좌엔 예약은 그대로 남는다) — 바운드된 잔여 gap이다. publisher 스레드가
 * Aeron {@code offer}에 계속 실패해도 마찬가지로 로그만 남긴다. 이 gap을 없애는 보장 전달(never
 * drop)은 2단계(입력 저널 + Aeron Archive 리플레이)에서 닫는다 — 이 유닛은 구조 분리까지다.</p>
 *
 * <p>코어는 시간을 모르므로 주문 시각(orderAt)은 {@link #forwardPlace}에서(placement 시각, 가장자리)
 * {@link Instant#now()}로 직접 찍어 큐에 넣는다(Clock 주입 금지).</p>
 *
 * <p>범위는 인프로세스까지다 — 실제 두 프로세스(account-worker↔matching-worker) 연결과
 * accountId/stockCode 라우팅은 C5-4에서 다룬다.</p>
 */
@Slf4j
public class AeronMatchingOrderSender implements MatchingOrderSender, AutoCloseable {

    private static final int ENCODE_BUFFER_SIZE = 256;
    private static final int OUTBOX_CAPACITY = 65536;
    private static final long CLOSE_JOIN_TIMEOUT_MILLIS = 1000L;

    private final Publication matchingOrderPublication;
    private final OrderCodec codec = new OrderCodec();
    private final OneToOneConcurrentArrayQueue<JournaledOrder> outbox = new OneToOneConcurrentArrayQueue<>(OUTBOX_CAPACITY);
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();
    private final AtomicBoolean running = new AtomicBoolean(false);

    private Thread publisherThread;

    public AeronMatchingOrderSender(Publication matchingOrderPublication) {
        this.matchingOrderPublication = matchingOrderPublication;
    }

    /** 계좌 컨슈머 스레드가 호출한다 — 큐에 넣기만 하고 즉시 돌아간다(논블로킹, 예외 없음). */
    @Override
    public void forwardPlace(long orderId, long accountId, String stockCode, OrderSide side, BigDecimal price, int quantity) {
        JournaledOrder order = new JournaledOrder(
            EventType.PLACE, orderId, accountId, stockCode, side, price, quantity, Instant.now());

        if (!outbox.offer(order)) {
            log.warn("[계좌] 매칭 발신 큐가 가득 차 이번 주문을 버림(best-effort): orderId={} accountId={}", orderId, accountId);
        }
    }

    /** publisher 스레드를 기동한다. */
    public void start() {
        running.set(true);
        publisherThread = new Thread(this::publishLoop, "aeron-matching-order-publisher");
        publisherThread.setDaemon(true);
        publisherThread.start();
    }

    private void publishLoop() {
        while (running.get()) {
            JournaledOrder order = outbox.poll();
            if (order != null) {
                publish(order);
            }
            idleStrategy.idle(order != null ? 1 : 0);
        }
    }

    /** 전용 스레드에서만 돈다 — 여기서는 블로킹 재시도해도 계좌 로직 스레드를 막지 않는다. */
    private void publish(JournaledOrder order) {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(ENCODE_BUFFER_SIZE));
        int length = codec.encode(buffer, 0, order);

        long result = matchingOrderPublication.offer(buffer, 0, length);
        if (result <= 0) {
            log.warn("[계좌] 매칭 발신 실패(best-effort): orderId={} accountId={} offer 반환={}",
                order.orderId(), order.accountId(), result);
        }
    }

    /** publisher 스레드를 멈추고 종료를 기다린다. 큐에 남은 주문은 드레인하지 않는다(best-effort). */
    @Override
    public void close() {
        running.set(false);
        if (publisherThread != null) {
            try {
                publisherThread.join(CLOSE_JOIN_TIMEOUT_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
