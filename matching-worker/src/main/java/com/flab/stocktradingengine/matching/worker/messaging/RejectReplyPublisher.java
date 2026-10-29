package com.flab.stocktradingengine.matching.worker.messaging;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;

import com.flab.stocktradingengine.aeron.AccountDestinationResolver;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.codec.OrderCodec;
import com.flab.stocktradingengine.matching.disruptor.io.NotOwnedOrderListener;

import io.aeron.ExclusivePublication;
import io.aeron.Publication;

import lombok.extern.slf4j.Slf4j;

/**
 * 담당이 아니라 접수하지 않은 주문을, 그 주문의 계좌번호로 정해진 계좌 shard에 거부 회신 스트림
 * ({@code AeronStreamIds.REJECT_REPLY})으로 되돌린다(4-5절, 11-3절). 회신은 기존 {@link OrderCodec}의 PLACE
 * 메시지 그대로이고 사유 바이트는 없다. Archive 녹화를 걸지 않는다.
 *
 * <h3>두 thread</h3>
 * <p>{@link #onNotOwned}는 주문 수신 폴링 thread({@code aeron-order-receiver})가 부르고, 주문을 queue
 * ({@link OneToOneConcurrentArrayQueue}, 용량 {@value #DEFAULT_QUEUE_CAPACITY})에 넣는다. queue가 가득 차면
 * 버리지 않고 빈 칸이 생길 때까지 기다린다(11-2절). 이때 그 pod가 담당하는 종목 slot의 주문 수신도 같이 멈춘다.
 * 발신 thread 하나가 queue에서 꺼내 인코딩하고 offer한다.</p>
 *
 * <h3>보낼 수 없을 때</h3>
 * <p>계좌 shard 주소를 아직 모르거나(배정 대기), 그 주소의 Publication이 없거나, offer가 NOT_CONNECTED·
 * BACK_PRESSURED 등 일시적 실패면 그 주문을 발신 thread 안의 대기 목록에 두고 다음 회차에 다시 시도한다.
 * 버리지 않는다(버리면 계좌 shard에 예약이 남는다). 대기 목록이 있는 동안 새로 꺼낸 주문도 대기 목록 뒤에
 * 붙여 들어온 순서를 지킨다. 대기 목록의 크기와 기다리는 한도는 측정 전이라 정하지 않았다.
 * offer가 CLOSED·MAX_POSITION_EXCEEDED를 돌려주면 ERROR 로그를 남기고 대기 목록에 둔다.</p>
 *
 * <p>계측: queue 사용량 최댓값({@link #maxQueueDepth}), 넣는 쪽이 기다린 횟수({@link #enqueueWaitCount})와
 * 시간({@link #enqueueWaitNanos}).</p>
 */
@Slf4j
public class RejectReplyPublisher implements NotOwnedOrderListener {

    static final int DEFAULT_QUEUE_CAPACITY = 65536;
    private static final int ENCODE_BUFFER_SIZE = 256;
    private static final long CLOSE_JOIN_TIMEOUT_MILLIS = 5000L;

    private final AccountDestinationResolver destinationResolver;
    private final Map<String, ExclusivePublication> publicationsByEndpoint;
    private final OneToOneConcurrentArrayQueue<JournaledOrder> queue;
    private final OrderCodec codec = new OrderCodec();
    private final UnsafeBuffer encodeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(ENCODE_BUFFER_SIZE));
    private final Deque<JournaledOrder> pending = new ArrayDeque<>();
    private final IdleStrategy enqueueIdleStrategy = new BackoffIdleStrategy();
    private final IdleStrategy senderIdleStrategy = new BackoffIdleStrategy();
    private final AtomicBoolean running = new AtomicBoolean(false);

    private final AtomicLong maxQueueDepth = new AtomicLong();
    private final AtomicLong enqueueWaitCount = new AtomicLong();
    private final AtomicLong enqueueWaitNanos = new AtomicLong();

    private Thread senderThread;

    public RejectReplyPublisher(AccountDestinationResolver destinationResolver,
                                Map<String, ExclusivePublication> publicationsByEndpoint) {
        this(destinationResolver, publicationsByEndpoint, DEFAULT_QUEUE_CAPACITY);
    }

    RejectReplyPublisher(AccountDestinationResolver destinationResolver,
                         Map<String, ExclusivePublication> publicationsByEndpoint, int queueCapacity) {
        this.destinationResolver = destinationResolver;
        this.publicationsByEndpoint = publicationsByEndpoint;
        this.queue = new OneToOneConcurrentArrayQueue<>(queueCapacity);
    }

    /** 주문 수신 폴링 thread가 부른다. queue가 가득 차면 빈 칸이 생길 때까지 기다린다. */
    @Override
    public void onNotOwned(JournaledOrder order) {
        if (!queue.offer(order)) {
            long waitStartedAtNanos = System.nanoTime();
            enqueueWaitCount.incrementAndGet();
            enqueueIdleStrategy.reset();
            while (!queue.offer(order)) {
                enqueueIdleStrategy.idle();
            }
            enqueueWaitNanos.addAndGet(System.nanoTime() - waitStartedAtNanos);
        }
        maxQueueDepth.accumulateAndGet(queue.size(), Math::max);
    }

    public void start() {
        running.set(true);
        senderThread = new Thread(this::senderLoop, "reject-reply-sender");
        senderThread.setDaemon(true);
        senderThread.start();
    }

    private void senderLoop() {
        while (running.get() || !queue.isEmpty() || !pending.isEmpty()) {
            int sent = retryPending();
            JournaledOrder order = queue.poll();
            if (order != null) {
                if (!pending.isEmpty() || !trySend(order)) {
                    pending.addLast(order);
                } else {
                    sent++;
                }
            }
            senderIdleStrategy.idle(sent + (order != null ? 1 : 0));
            if (!running.get() && queue.isEmpty() && order == null && sent == 0) {
                // 닫는 중에 남은 대기 목록은 더 시도해도 나가지 않으므로 개수를 남기고 끝낸다.
                break;
            }
        }
        if (!pending.isEmpty()) {
            log.error("[매칭] 거부 회신 {}건을 보내지 못한 채 발신 thread가 끝났습니다", pending.size());
        }
    }

    /** 대기 목록을 앞에서부터 시도해, 보낸 건수를 돌려준다. 보내지 못한 주문은 순서를 그대로 둔다. */
    private int retryPending() {
        int sent = 0;
        Iterator<JournaledOrder> iterator = pending.iterator();
        while (iterator.hasNext()) {
            JournaledOrder order = iterator.next();
            if (trySend(order)) {
                iterator.remove();
                sent++;
            }
        }
        return sent;
    }

    private boolean trySend(JournaledOrder order) {
        Optional<String> endpoint = destinationResolver.fillEndpointFor(order.accountId());
        if (endpoint.isEmpty()) {
            return false;
        }
        ExclusivePublication publication = publicationsByEndpoint.get(endpoint.get());
        if (publication == null) {
            return false;
        }
        int length = codec.encode(encodeBuffer, 0, order);
        long result = publication.offer(encodeBuffer, 0, length);
        if (result >= 0) {
            return true;
        }
        if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
            log.error("[매칭] 거부 회신 발행 스트림이 복구 불가 상태입니다(offer 결과={}, 목적지={}, orderId={})",
                result, endpoint.get(), order.orderId());
        }
        return false;
    }

    /** 발신 thread를 멈추고 남은 회신을 보낼 시간을 준다. */
    public void close() {
        running.set(false);
        if (senderThread == null) {
            return;
        }
        try {
            senderThread.join(CLOSE_JOIN_TIMEOUT_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public long maxQueueDepth() {
        return maxQueueDepth.get();
    }

    public long enqueueWaitCount() {
        return enqueueWaitCount.get();
    }

    public long enqueueWaitNanos() {
        return enqueueWaitNanos.get();
    }
}
