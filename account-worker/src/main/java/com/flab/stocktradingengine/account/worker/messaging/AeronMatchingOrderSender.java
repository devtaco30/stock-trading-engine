package com.flab.stocktradingengine.account.worker.messaging;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;

import com.flab.stocktradingengine.account.disruptor.io.MatchingOrderSender;
import com.flab.stocktradingengine.account.worker.config.MatchingOrderSenderConfig;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.codec.OrderCodec;
import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.Publication;

import lombok.extern.slf4j.Slf4j;

/**
 * {@link MatchingOrderSender}의 Aeron 구현 — 계좌가 accept한 매수·매도를 매칭 인테이크
 * (파이프라인 연결 ①, stream {@link com.flab.stocktradingengine.aeron.AeronStreamIds#MATCHING_INTAKE})로
 * 인코딩해 보낸다. matching-worker {@code FillOutbox}(I4)의 대칭 — 목적지가 하나뿐이라(
 * {@code MatchingOrderSenderConfig} 참고) 그쪽처럼 endpoint별로 나누지 않고 이 클래스 하나가 큐·전용
 * 스레드·인코딩 버퍼를 직접 갖는다(I2 발신측 D5).
 *
 * <h3>발신을 로직 스레드 밖으로 (LMAX 1단계)</h3>
 * <p>{@link #forwardPlace}는 계좌 엔진의 단일 상시 컨슈머 스레드에서 호출된다 — 그 스레드가 직접
 * Aeron {@code offer}(네트워크 I/O)를 하면, single-writer가 I/O로 블로킹·중단될 수 있다(LMAX 원칙
 * 위반). 그래서 {@code forwardPlace}는 {@link #outbox}(SPSC 락프리 큐, 프로듀서=계좌 스레드 하나·
 * 컨슈머=전용 {@link #publisherThread} 하나)에 큐잉만 하고 실제 인코딩·Aeron 발신은
 * {@link #publisherThread}가 한다.</p>
 *
 * <h3>버리지 않는다(D1·D2)</h3>
 * <p>큐가 가득 차면(용량={@value #OUTBOX_CAPACITY}) {@link #forwardPlace}가 버리는 대신 그 자리에서
 * 대기한다 — 대기하는 주체가 계좌의 단일 쓰기 스레드라, 큐가 가득 찬 동안은 이 샤드가 맡은 다른
 * 계좌의 주문 접수도 같이 멈춘다. 이 대가를 받아들이는 근거: 매칭이 주문을 못 받는 상태라면 그
 * 종목의 거래는 어차피 성립하지 않는다 — 접수만 계속 받아 봐야 예약만 쌓이고 체결은 안 난다.
 * matching-worker {@code FillOutbox}(목적지 여럿, 하나가 막혀도 나머지를 살리는 게 핵심)와 달리
 * 여기는 목적지가 하나라 그 구분이 없다. 목적지가 영구히 안 살아나는 경우(계좌 프로세스가 다시 안
 * 뜨는 것)까지 닫는 것은 이 유닛 밖이다(대기 프로세스를 두는 I8의 몫). {@link #publisherThread}는
 * Aeron {@code offer}가 성공(반환값 ≥ 0)할 때까지 재시도한다 — 전용 스레드라 여기서 기다려도 계좌
 * 로직을 막지 않는다.</p>
 *
 * <h3>일시적 실패와 복구 불가 실패를 다르게 다룬다(D3)</h3>
 * <p>{@code BACK_PRESSURED} 같은 일시적 실패는 {@link #publisherThread}가 조용히 계속 재시도한다.
 * 반면 {@code offer}가 {@code CLOSED}·{@code MAX_POSITION_EXCEEDED}를 돌려주면 이 목적지로는 다시
 * 보낼 수 없다는 뜻이라 성격이 다르다 — {@link #fatalError}에 예외를 기록하고 {@link #publisherThread}가
 * 그 예외로 죽는다(ERROR 로그로 남음). 이 스레드는 Disruptor 핸들러가 아니라 예외를 던져도 계좌
 * 소비자 스레드까지 자동으로 전파되지 않으므로, {@link #forwardPlace}가 호출될 때마다
 * {@link #fatalError}를 확인해 계좌 소비자 스레드 자신에서 다시 던진다 — 그래야
 * {@code AccountExceptionHandler}가 계좌 전체를 fail-fast로 멈춘다.</p>
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
    // 패키지 가시성 — 테스트가 이 값으로 큐를 정확히 채운다.
    static final int OUTBOX_CAPACITY = 65536;
    private static final long CLOSE_DRAIN_TIMEOUT_MILLIS = 5000L;

    private final Publication matchingOrderPublication;
    private final OrderCodec codec = new OrderCodec();
    private final OneToOneConcurrentArrayQueue<JournaledOrder> outbox = new OneToOneConcurrentArrayQueue<>(OUTBOX_CAPACITY);
    private final IdleStrategy enqueueIdleStrategy = new BackoffIdleStrategy();
    private final IdleStrategy pollIdleStrategy = new BackoffIdleStrategy();
    private final IdleStrategy offerIdleStrategy = new BackoffIdleStrategy();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<IllegalStateException> fatalError = new AtomicReference<>();
    // publish()가 publisherThread 하나만 호출하는 단일 writer라 인코딩 버퍼를 필드로 재사용해도
    // 안전하다(호출마다 새로 allocateDirect하면 off-heap 네이티브 메모리가 누적된다).
    private final UnsafeBuffer encodeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(ENCODE_BUFFER_SIZE));

    private Thread publisherThread;

    public AeronMatchingOrderSender(Publication matchingOrderPublication) {
        this.matchingOrderPublication = matchingOrderPublication;
    }

    /**
     * 계좌 컨슈머 스레드가 호출한다. 큐가 가득 차면 버리지 않고 그 자리에서 기다린다(D1·D2). 발신이
     * 복구 불가 상태로 기록돼 있으면 큐에 넣지 않고 그 자리에서 예외를 던진다(D3) — 그래야 이 호출을
     * 부른 {@code AccountEventHandler}까지 예외가 올라가 {@code AccountExceptionHandler}가 계좌
     * 전체를 멈춘다.
     */
    @Override
    public void forwardPlace(long orderId, long accountId, String stockCode, OrderSide side, BigDecimal price, int quantity) {
        throwIfFatal();
        JournaledOrder order = new JournaledOrder(
            EventType.PLACE, orderId, accountId, stockCode, side, price, quantity, Instant.now());

        enqueueIdleStrategy.reset();
        while (!outbox.offer(order)) {
            throwIfFatal();
            enqueueIdleStrategy.idle();
        }
    }

    private void throwIfFatal() {
        IllegalStateException error = fatalError.get();
        if (error != null) {
            throw new IllegalStateException(error.getMessage(), error);
        }
    }

    /** publisher 스레드를 기동한다. */
    public void start() {
        running.set(true);
        publisherThread = new Thread(this::publishLoop, "aeron-matching-order-publisher");
        publisherThread.setDaemon(true);
        publisherThread.setUncaughtExceptionHandler((thread, error) ->
            log.error("[계좌] 매칭 발신 스레드가 복구 불가 상태로 멈췄습니다", error));
        publisherThread.start();
    }

    private void publishLoop() {
        while (running.get() || !outbox.isEmpty()) {
            JournaledOrder order = outbox.poll();
            if (order != null) {
                publish(order);
            }
            pollIdleStrategy.idle(order != null ? 1 : 0);
        }
    }

    /** 전용 스레드에서만 돈다 — 여기서 블로킹 재시도해도 계좌 로직 스레드를 막지 않는다. */
    private void publish(JournaledOrder order) {
        int length = codec.encode(encodeBuffer, 0, order);

        offerIdleStrategy.reset();
        long result;
        while ((result = matchingOrderPublication.offer(encodeBuffer, 0, length)) < 0) {
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                IllegalStateException error = new IllegalStateException(
                    "매칭 발행 스트림이 복구 불가 상태입니다(offer 결과=" + result
                        + "): 계좌가 이 주문을 영원히 못 보내므로 계좌 엔진을 멈춥니다");
                fatalError.set(error);
                throw error;
            }
            offerIdleStrategy.idle();
        }
    }

    /** publisher 스레드를 멈추고 큐에 남은 주문을 드레인한다(D4). Spring 빈 {@code destroyMethod}로 호출된다. */
    @Override
    public void close() {
        close(CLOSE_DRAIN_TIMEOUT_MILLIS);
    }

    /**
     * 패키지 가시성 — 테스트가 짧은 타임아웃으로 드레인 실패 경로를 검증한다. {@code drainTimeoutMillis}
     * 안에 드레인이 끝나지 않으면 남은 개수를 ERROR로 남긴다 — 조용히 사라지는 자리를 만들지 않는다.
     */
    void close(long drainTimeoutMillis) {
        running.set(false);
        if (publisherThread == null) {
            return;
        }

        try {
            publisherThread.join(drainTimeoutMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 스레드가 아직 살아 있다는 건 큐가 빈 뒤에도(publishLoop 종료 조건) 안 끝났다는 뜻이라,
        // 지금 발신 재시도 중인 주문 1건이 있다 — outbox.size()만 세면 그 1건이 빠진다. +1이 정확히
        // 1인 이유: publishLoop가 멈춰 있을 수 있는 자리는 publish() 안의 재시도 루프뿐이고, 그
        // 루프는 poll()로 큐에서 꺼낸 order 하나를 손에 든 채로 돈다 — 항상 정확히 한 건이다
        // (matching-worker FillOutbox.close()와 같은 계산, I4에서 이 +1을 빠뜨렸던 버그를 고친 뒤의 형태).
        if (publisherThread.isAlive()) {
            int remaining = outbox.size() + 1;
            log.error("[계좌] 매칭 발신 드레인이 타임아웃 안에 끝나지 않았습니다 — 유실 가능한 주문 {}건 남음", remaining);
        }
    }
}
