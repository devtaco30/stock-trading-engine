package com.flab.stocktradingengine.account.worker.messaging;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;

import com.flab.stocktradingengine.support.IdleStrategies;
import com.flab.stocktradingengine.account.disruptor.io.MatchingOrderSender;
import com.flab.stocktradingengine.account.worker.config.MatchingOrderSenderConfig;
import com.flab.stocktradingengine.account.worker.coordination.MatchingDestinationMisconfiguredException;
import com.flab.stocktradingengine.aeron.MatchingSlotTarget;
import com.flab.stocktradingengine.account.worker.coordination.MatchingTargetLookup;
import com.flab.stocktradingengine.account.worker.coordination.MatchingTargetView;
import com.flab.stocktradingengine.aeron.StockSlotHasher;
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
 * <h3>종목 slot으로 담당 매칭 pod를 고른다(매칭 샤딩 U9)</h3>
 * <p>{@link #publisherThread}는 queue 1개에서 꺼낸 주문의 종목코드로 {@link MatchingTargetLookup}에게
 * 담당 pod의 주소를 묻고, 주소 풀에서 만들어 둔 그 주소의 {@link Publication}으로 offer한다. 담당이
 * 없거나(첫 배정 전·pod가 죽음) 맵에 「이동 중」 표시가 있으면 주문을 그 종목 slot의 보관 목록에
 * 두고 다음 주문으로 넘어간다 — 다른 종목 slot의 주문은 막히지 않는다. 담당이 생기면 그 종목 slot의
 * 목록을 먼저 비운 뒤에 새 주문을 보내 순서를 유지한다. 보관 목록은 종목 slot마다 「재전송 대기
 * 목록(앞)」과 「신규 보관 목록(뒤)」으로 나뉜다. 매칭 pod가 담당이 아니라며 거부 회신으로 돌려보낸
 * 주문({@link #acceptRejectedOrder})은 재전송 대기 목록에 도착 순서대로 붙고 비울 때 먼저 나간다.
 * 보관 목록은 이 스레드만 만지고, 계좌 엔진 스레드는 개수 배열({@link #hasHoldingRoom})만 읽는다.</p>
 *
 * <p>queue가 1개라 한 pod의 Publication이 막히면 뒤의 다른 pod행 주문도 못 나간다(설계서 3-3절의
 * 시작 구조). 그 재시도 사이마다 거부 회신 입구를 비워 서로를 기다리는 순환을 끊는다(11-2절).
 * 풀 설정이 없는 기존 실행은 항상 같은 담당과 Publication 1개를 가진 조회로 표현해 같은 코드가
 * 그대로 지난다.</p>
 */
@Slf4j
public class AeronMatchingOrderSender implements MatchingOrderSender, AutoCloseable {

    private static final int ENCODE_BUFFER_SIZE = 256;
    // 패키지 가시성 — 테스트가 이 값으로 큐를 정확히 채운다.
    static final int OUTBOX_CAPACITY = 65536;
    private static final long CLOSE_DRAIN_TIMEOUT_MILLIS = 5000L;

    // 풀 설정이 없는 기존 실행(Publication 1개)을 같은 경로로 처리하기 위한 고정 담당.
    private static final String SINGLE_ENDPOINT = "single";
    static final int REJECTED_INBOX_CAPACITY = 65536;

    private final MatchingTargetLookup targetLookup;
    private final Map<String, Publication> publicationsByEndpoint;
    private final StockSlotHasher stockSlotHasher;
    private final int holdingCapacityPerSlot;
    // 종목 slot별 보관 중인 주문 개수. 보관 목록은 publisherThread만 만지고 개수만 계좌 엔진 스레드가 읽는다.
    private final AtomicIntegerArray holdingCounts;
    private final SlotHold[] slotHolds;
    // 거부 회신을 받는 poll 스레드(생산자 하나)가 넣고 publisherThread(소비자 하나)가 꺼낸다.
    private final OneToOneConcurrentArrayQueue<JournaledOrder> rejectedInbox = new OneToOneConcurrentArrayQueue<>(REJECTED_INBOX_CAPACITY);
    private final IdleStrategy rejectedEnqueueIdleStrategy = new BackoffIdleStrategy();
    private int heldOrderTotal;
    // 거부 회신으로 돌아온 주문의 재전송 간격. 첫 거부는 바로 보내고, 같은 종목 slot에서 거부가 이어지면 기본 간격부터 두 배씩 늘려 상한까지 둔다.
    // 매칭 pod가 재시작해 맵에는 평상시 값이 남았는데 담당 표시는 꺼져 있는 동안 거부와 재전송이 쉬지 않고 도는 것을 막는다. 값은 publisherThread만 읽고
    // 시험이 시작 전에 바꾼다.
    private long retransmitBackoffBaseNanos = 20_000_000L;
    private long retransmitBackoffCapNanos = 1_000_000_000L;
    private long retransmitResetNanos = 2_000_000_000L;
    // 계측(설계서 3-6절) — 계좌 엔진 스레드와 publisherThread가 올리고 리포터 스레드가 읽는다.
    private final LongAdder enqueueWaitCount = new LongAdder();
    private final LongAdder enqueueWaitNanos = new LongAdder();
    private final LongAdder offerRetryCount = new LongAdder();
    // 매칭 pod가 「담당이 아니다」라며 돌려보낸 회신을 받은 횟수(이벤트 단위, 같은 주문이 여러 번 거부되면 여러 번 센다).
    private final LongAdder rejectedReplyCount = new LongAdder();
    // 첫 거부 회신과 마지막 거부 회신을 받은 시각(epoch ms, 받기 전에는 0). 시험이 이동 중 구간과 겹치는지 본다.
    private final java.util.concurrent.atomic.AtomicLong firstRejectedReplyEpochMillis = new java.util.concurrent.atomic.AtomicLong();
    private volatile long lastRejectedReplyEpochMillis;
    // 보관 목록에 동시에 있었던 주문 수의 최대(publisherThread가 올린다).
    private volatile int maxHeldOrderTotal;
    private final OrderCodec codec = new OrderCodec();
    private final OneToOneConcurrentArrayQueue<JournaledOrder> outbox = new OneToOneConcurrentArrayQueue<>(OUTBOX_CAPACITY);
    private final IdleStrategy enqueueIdleStrategy = new BackoffIdleStrategy();
    private final IdleStrategy pollIdleStrategy;
    private final IdleStrategy offerIdleStrategy = new BackoffIdleStrategy();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<IllegalStateException> fatalError = new AtomicReference<>();
    // publish()가 publisherThread 하나만 호출하는 단일 writer라 인코딩 버퍼를 필드로 재사용해도
    // 안전하다(호출마다 새로 allocateDirect하면 off-heap 네이티브 메모리가 누적된다).
    private final UnsafeBuffer encodeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(ENCODE_BUFFER_SIZE));

    private Thread publisherThread;

    public AeronMatchingOrderSender(Publication matchingOrderPublication) {
        this(matchingOrderPublication, IdleStrategies.forThread());
    }

    /**
     * 쉬는 방식 변경 실험(EXPERIMENT_IDLE)이 발신 스레드의 큐 폴링 쉬는 방식(주문 이동 경로의 대기 지점)을 바꿀 수 있게
     * 전략을 주입받는다. 큐가 가득 찼을 때와 offer가 실패했을 때만 쓰는 나머지 두 전략은 바꾸지 않는다.
     */
    public AeronMatchingOrderSender(Publication matchingOrderPublication, IdleStrategy pollIdleStrategy) {
        this(() -> stockCode -> Optional.of(new MatchingSlotTarget(SINGLE_ENDPOINT, false)),
            Map.of(SINGLE_ENDPOINT, matchingOrderPublication), new StockSlotHasher(1), Integer.MAX_VALUE, pollIdleStrategy);
    }

    public AeronMatchingOrderSender(MatchingTargetLookup targetLookup, Map<String, Publication> publicationsByEndpoint,
                                    StockSlotHasher stockSlotHasher, int holdingCapacityPerSlot) {
        this(targetLookup, publicationsByEndpoint, stockSlotHasher, holdingCapacityPerSlot, IdleStrategies.forThread());
    }

    /**
     * @param publicationsByEndpoint 주소 풀의 주소마다 미리 열어 둔 Publication(아직 뜨지 않은 pod의 것은 연결 전 상태)
     * @param holdingCapacityPerSlot 종목 slot 하나의 보관 목록에 둘 수 있는 주문 수. 이 수에 이르면 {@link #hasHoldingRoom}이 false
     */
    public AeronMatchingOrderSender(MatchingTargetLookup targetLookup, Map<String, Publication> publicationsByEndpoint,
                                    StockSlotHasher stockSlotHasher, int holdingCapacityPerSlot, IdleStrategy pollIdleStrategy) {
        this.targetLookup = targetLookup;
        this.publicationsByEndpoint = Map.copyOf(publicationsByEndpoint);
        this.stockSlotHasher = stockSlotHasher;
        this.holdingCapacityPerSlot = holdingCapacityPerSlot;
        this.holdingCounts = new AtomicIntegerArray(stockSlotHasher.stockSlotCount());
        this.slotHolds = new SlotHold[stockSlotHasher.stockSlotCount()];
        this.pollIdleStrategy = pollIdleStrategy;
    }

    /** 이 종목 slot의 보관 목록에 새 주문을 더 받을 여유가 있는가. 계좌 엔진 스레드가 접수 전에 부른다. */
    public boolean hasHoldingRoom(String stockCode) {
        int slot = stockSlotHasher.slotFor(stockCode);
        return holdingCounts.get(slot) < holdingCapacityPerSlot;
    }

    public long firstRejectedReplyEpochMillis() {
        return firstRejectedReplyEpochMillis.get();
    }

    public long lastRejectedReplyEpochMillis() {
        return lastRejectedReplyEpochMillis;
    }

    /** 보관 목록(재전송 대기 + 신규)에 동시에 있었던 주문 수의 최대. */
    public int maxHeldOrderTotal() {
        return maxHeldOrderTotal;
    }

    /** 테스트용 — 재전송 간격(기본, 상한)과 거부가 없으면 횟수를 되돌리는 시간을 바꾼다. start() 전에만 부른다. */
    void setRetransmitBackoff(long baseMillis, long capMillis, long resetMillis) {
        this.retransmitBackoffBaseNanos = baseMillis * 1_000_000L;
        this.retransmitBackoffCapNanos = capMillis * 1_000_000L;
        this.retransmitResetNanos = resetMillis * 1_000_000L;
    }

    /** 같은 종목 slot에서 {@code rejectRound}번째 연속 거부 뒤 재전송까지 두는 간격. 첫 거부는 0, 이후 기본 간격에서 두 배씩 늘어 상한에서 멈춘다. */
    static long retransmitDelayNanos(int rejectRound, long baseNanos, long capNanos) {
        if (rejectRound <= 1) {
            return 0L;
        }
        int shift = rejectRound - 2;
        if (shift >= 32 || (baseNanos << shift) >= capNanos || (baseNanos << shift) < 0) {
            return capNanos;
        }
        return baseNanos << shift;
    }

    /** 테스트용 — 이 종목 slot의 보관 목록에 있는 주문 수(재전송 대기 + 신규). */
    int holdingCount(String stockCode) {
        return holdingCounts.get(stockSlotHasher.slotFor(stockCode));
    }

    /** 매칭 pod의 「담당이 아니다」 거부 회신을 받은 누적 횟수. 종료할 때 INFO 로그로도 남는다. */
    public long rejectedReplyCount() {
        return rejectedReplyCount.sum();
    }

    /** 테스트용 — 아직 보관 목록으로 옮기지 않은 거부 회신 주문 수. */
    int rejectedInboxSize() {
        return rejectedInbox.size();
    }

    /**
     * 매칭 pod가 담당이 아니라며 돌려보낸 주문을 받는다. 거부 회신을 읽는 poll 스레드 하나만 부른다
     * (1:1 queue의 생산자). 가득 차면 버리지 않고 기다린다(설계서 11-2절 가).
     */
    public void acceptRejectedOrder(JournaledOrder order) {
        throwIfFatal();
        rejectedReplyCount.increment();
        long receivedAt = System.currentTimeMillis();
        lastRejectedReplyEpochMillis = receivedAt;
        if (log.isDebugEnabled()) {
            // 거부 회신마다 시각을 남긴다(시험이 이동 중 표시 구간 안의 거부 건수를 직접 세는 데 쓴다). DEBUG를 켠 시험에서만 출력된다.
            log.debug("[계좌] 「담당 아님」 거부 회신 수신: 시각(epoch ms)={}", receivedAt);
        }
        if (firstRejectedReplyEpochMillis.compareAndSet(0L, receivedAt)) {
            // 강제 종료되어 종료 로그가 없어도 이 줄은 남는다.
            log.info("[계좌] 매칭 pod의 「담당 아님」 거부 회신을 처음 받음: 시각(epoch ms)={}", receivedAt);
        }
        rejectedEnqueueIdleStrategy.reset();
        while (!rejectedInbox.offer(order)) {
            throwIfFatal();
            rejectedEnqueueIdleStrategy.idle();
        }
    }

    /**
     * 계좌 컨슈머 스레드가 호출한다. 큐가 가득 차면 버리지 않고 그 자리에서 기다린다(D1·D2). 발신이
     * 복구 불가 상태로 기록돼 있으면 큐에 넣지 않고 그 자리에서 예외를 던진다(D3) — 그래야 이 호출을
     * 부른 {@code AccountEventHandler}까지 예외가 올라가 {@code AccountExceptionHandler}가 계좌
     * 전체를 멈춘다.
     */
    @Override
    public void forwardPlace(long orderId, long accountId, String stockCode, OrderSide side, BigDecimal price, int quantity) {
        forwardPlace(orderId, accountId, stockCode, side, price, quantity, 0L, 0L, 0L);
    }

    /** 구간 분해 측정용 — api 발신 시각·수신 시각·승인 후 전달 직전 시각을 와이어에 싣는다. */
    @Override
    public void forwardPlace(long orderId, long accountId, String stockCode, OrderSide side, BigDecimal price, int quantity,
                             long originNanos, long receivedAtNanos, long acceptedAtNanos) {
        throwIfFatal();
        JournaledOrder order = new JournaledOrder(
            EventType.PLACE, orderId, accountId, stockCode, side, price, quantity, Instant.now(),
            originNanos, receivedAtNanos, acceptedAtNanos);

        if (outbox.offer(order)) {
            return;
        }
        // queue가 가득 차 기다린다 — 기다린 횟수와 시간을 센다.
        long waitStartNanos = System.nanoTime();
        enqueueIdleStrategy.reset();
        while (!outbox.offer(order)) {
            throwIfFatal();
            enqueueIdleStrategy.idle();
        }
        enqueueWaitNanos.add(System.nanoTime() - waitStartNanos);
        enqueueWaitCount.increment();
    }

    /** 매칭행 발신에서 센 값의 지금 모습. 리포터 스레드가 읽는다. */
    public MatchingSendMetrics metrics() {
        return new MatchingSendMetrics(outbox.size(), enqueueWaitCount.sum(), enqueueWaitNanos.sum(), offerRetryCount.sum());
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
            // 한 회차의 모든 판단(보낼지·보관할지·보관 목록을 비울지)에 같은 view를 쓴다(설계서 11-1절).
            MatchingTargetView view = targetLookup.view();
            drainRejectedInbox();
            boolean flushed = flushAvailableHolds(view);
            JournaledOrder order = outbox.poll();
            if (order != null) {
                route(view, order);
            }
            pollIdleStrategy.idle(order != null || flushed ? 1 : 0);
        }
    }

    private void route(MatchingTargetView view, JournaledOrder order) {
        MatchingSlotTarget target = findTarget(view, order.stockCode());
        if (target == null || target.moving() || slotHasHeldOrders(order.stockCode())) {
            // 같은 종목 slot에 재전송 대기 중인 주문이 있으면(재전송 간격 동안) 새 주문이 앞질러 나가지 않도록 뒤에 보관한다.
            hold(order, false);
            return;
        }
        publish(target.endpoint(), order);
    }

    private boolean slotHasHeldOrders(String stockCode) {
        SlotHold hold = slotHolds[stockSlotHasher.slotFor(stockCode)];
        return hold != null && !hold.isEmpty();
    }

    /** 담당이 생긴 종목 slot의 보관 목록을 비운다. 재전송 대기 목록을 먼저, 신규 보관 목록을 그다음에 보낸다. */
    private boolean flushAvailableHolds(MatchingTargetView view) {
        if (heldOrderTotal == 0) {
            return false;
        }
        boolean flushed = false;
        for (int slot = 0; slot < slotHolds.length; slot++) {
            SlotHold hold = slotHolds[slot];
            if (hold == null || hold.isEmpty()) {
                continue;
            }
            MatchingSlotTarget target = findTarget(view, hold.peekFirst().stockCode());
            if (target == null || target.moving()) {
                continue;
            }
            if (System.nanoTime() - hold.notBeforeNanos < 0) {
                continue; // 재전송 간격이 아직 지나지 않았다
            }
            hold.rejectedSinceFlush = false;
            JournaledOrder held;
            while ((held = hold.pollFirst()) != null) {
                publish(target.endpoint(), held);
                heldOrderTotal--;
                holdingCounts.decrementAndGet(slot);
            }
            flushed = true;
        }
        return flushed;
    }

    private void hold(JournaledOrder order, boolean retransmit) {
        int slot = stockSlotHasher.slotFor(order.stockCode());
        SlotHold hold = slotHolds[slot];
        if (hold == null) {
            hold = new SlotHold();
            slotHolds[slot] = hold;
        }
        if (retransmit) {
            hold.retransmit.addLast(order);
            long now = System.nanoTime();
            if (!hold.rejectedSinceFlush) {
                // 한 번 비운 뒤 처음 돌아온 거부 회신만 새 연속 거부 한 회차로 센다(같은 회차에 몰려 온 회신은 한 회차).
                if (now - hold.lastRejectNanos > retransmitResetNanos) {
                    hold.consecutiveRejects = 0;
                }
                hold.consecutiveRejects++;
                hold.rejectedSinceFlush = true;
                hold.notBeforeNanos = now + retransmitDelayNanos(hold.consecutiveRejects, retransmitBackoffBaseNanos, retransmitBackoffCapNanos);
            }
            hold.lastRejectNanos = now;
        } else {
            hold.fresh.addLast(order);
        }
        heldOrderTotal++;
        if (heldOrderTotal > maxHeldOrderTotal) {
            maxHeldOrderTotal = heldOrderTotal;
        }
        holdingCounts.incrementAndGet(slot);
    }

    private void drainRejectedInbox() {
        rejectedInbox.drain(order -> hold(order, true));
    }

    private MatchingSlotTarget findTarget(MatchingTargetView view, String stockCode) {
        try {
            return view.targetFor(stockCode).orElse(null);
        } catch (MatchingDestinationMisconfiguredException e) {
            IllegalStateException error = new IllegalStateException(
                "매칭 담당 주소가 설정과 어긋납니다: 계좌 엔진을 멈춥니다 — " + e.getMessage(), e);
            fatalError.set(error);
            throw error;
        }
    }

    /** 전용 스레드에서만 돈다 — 여기서 블로킹 재시도해도 계좌 로직 스레드를 막지 않는다. */
    private void publish(String endpoint, JournaledOrder order) {
        Publication publication = publicationsByEndpoint.get(endpoint);
        if (publication == null) {
            IllegalStateException error = new IllegalStateException(
                "주소 풀에 Publication이 없는 주소입니다: " + endpoint + " — 계좌 엔진을 멈춥니다");
            fatalError.set(error);
            throw error;
        }
        int length = codec.encode(encodeBuffer, 0, order);

        offerIdleStrategy.reset();
        long result;
        while ((result = publication.offer(encodeBuffer, 0, length)) < 0) {
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                IllegalStateException error = new IllegalStateException(
                    "매칭 발행 스트림이 복구 불가 상태입니다(offer 결과=" + result
                        + "): 계좌가 이 주문을 영원히 못 보내므로 계좌 엔진을 멈춥니다");
                fatalError.set(error);
                throw error;
            }
            offerRetryCount.increment();
            // 이 재시도에 묶여 있어도 거부 회신 입구는 계속 비운다 — 매칭 pod가 주문을 못 읽어 거부 회신을
            // 못 내보내는 순환 대기를 끊는다(설계서 11-2절).
            drainRejectedInbox();
            offerIdleStrategy.idle();
        }
    }

    /** 종목 slot 하나의 보관 목록. 재전송 대기 목록이 신규 보관 목록보다 앞에 나간다. */
    private static final class SlotHold {
        final ArrayDeque<JournaledOrder> retransmit = new ArrayDeque<>();
        final ArrayDeque<JournaledOrder> fresh = new ArrayDeque<>();
        int consecutiveRejects;
        boolean rejectedSinceFlush;
        long lastRejectNanos = Long.MIN_VALUE / 2;
        long notBeforeNanos;

        boolean isEmpty() {
            return retransmit.isEmpty() && fresh.isEmpty();
        }

        JournaledOrder peekFirst() {
            return retransmit.isEmpty() ? fresh.peekFirst() : retransmit.peekFirst();
        }

        JournaledOrder pollFirst() {
            return retransmit.isEmpty() ? fresh.pollFirst() : retransmit.pollFirst();
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
        int heldRemaining = 0;
        for (int slot = 0; slot < holdingCounts.length(); slot++) {
            heldRemaining += holdingCounts.get(slot);
        }
        log.info("[계좌] 매칭 발신 종료: 담당 아님 거부 회신 수신 {}건(첫 수신 {} 마지막 수신 {} epoch ms), 보관 목록 최대 {}건, 보관 목록에 남은 주문 {}건",
            rejectedReplyCount.sum(), firstRejectedReplyEpochMillis.get(), lastRejectedReplyEpochMillis, maxHeldOrderTotal, heldRemaining);
        if (heldRemaining > 0 && !publisherThread.isAlive()) {
            log.error("[계좌] 담당 매칭 pod를 찾지 못해 보관 중이던 주문 {}건이 종료와 함께 사라집니다", heldRemaining);
        }
    }
}
