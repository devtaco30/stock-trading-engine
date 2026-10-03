package com.flab.stocktradingengine.api.messaging;

import java.util.concurrent.locks.LockSupport;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;

import com.flab.stocktradingengine.aeron.AccountDestinationResolver;
import com.flab.stocktradingengine.api.exception.OrderPublishException;

import lombok.extern.slf4j.Slf4j;

/**
 * v2 api를 「받으면 곧바로 202」로 만드는 전달 큐와 전용 스레드.
 *
 * <p>HTTP 스레드는 {@link #submit}으로 검증이 끝난 주문을 큐에 넣고 바로 돌아온다. 큐에서 꺼내 Aeron으로 보내고 재시도하는 일은
 * {@code order-dispatcher} 스레드 하나가 맡는다. 끝내 전달하지 못한 주문은 Redis 기록과 ERROR 로그(주문 전 필드)로 남는다. 202를 준 뒤
 * 프로세스가 비정상 종료하면 큐에 있던 주문은 사라질 수 있다(v1의 Kafka 프로듀서 버퍼에도 같은 창이 있다).</p>
 *
 * <p>큐는 다중 생산자·단일 소비자 배열 큐이고 용량은 2의 거듭제곱으로 올려 잡힌다. 큐가 가득 차면 HTTP 스레드를 막지 않고 503을 준다.</p>
 */
@Slf4j
public class AccountOrderDispatcher implements AutoCloseable {

    private static final long DRAIN_WAIT_ON_CLOSE_MILLIS = 5_000;
    private static final long MAX_BACKOFF_NANOS = 50_000_000L;
    private static final long FIRST_BACKOFF_NANOS = 1_000_000L;

    private final AccountDestinationResolver destinationResolver;
    private final AeronAccountOrderSender sender;
    private final OrderDeliveryFailureStore failureStore;
    private final ManyToOneConcurrentArrayQueue<PendingOrder> queue;
    private final long deadlineMillis;
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();

    private volatile boolean accepting = true;
    private volatile boolean running;
    private Thread dispatchThread;

    public AccountOrderDispatcher(
            AccountDestinationResolver destinationResolver, AeronAccountOrderSender sender,
            OrderDeliveryFailureStore failureStore, int queueCapacity, long deadlineMillis) {
        this.destinationResolver = destinationResolver;
        this.sender = sender;
        this.failureStore = failureStore;
        this.queue = new ManyToOneConcurrentArrayQueue<>(queueCapacity);
        this.deadlineMillis = deadlineMillis;
    }

    /**
     * HTTP 스레드에서 불린다. 목적지가 배정됐는지 메모리에서 확인하고 큐에 넣은 뒤 돌아온다.
     *
     * @throws OrderPublishException 종료 중이거나, 목적지가 아직 없거나, 큐가 가득 차서 큐에 넣지 못했을 때(202를 주지 않는다)
     */
    public void submit(PendingOrder order) {
        if (!accepting) {
            throw new OrderPublishException("api가 종료 중이라 주문을 받을 수 없습니다: requestId=" + order.requestId());
        }
        if (destinationResolver.orderEndpointFor(order.accountId()).isEmpty()) {
            throw new OrderPublishException(
                "계좌 인테이크 fan-out 목적지를 아직 찾을 수 없습니다(배정 대기 중일 수 있음): accountId=" + order.accountId());
        }
        if (!queue.offer(order)) {
            throw new OrderPublishException("주문 전달 큐가 가득 찼습니다: accountId=" + order.accountId() + " requestId=" + order.requestId());
        }
    }

    public void start() {
        running = true;
        dispatchThread = new Thread(this::dispatchLoop, "order-dispatcher");
        dispatchThread.setDaemon(true);
        dispatchThread.start();
    }

    private void dispatchLoop() {
        while (running) {
            PendingOrder order = queue.poll();
            if (order == null) {
                idleStrategy.idle();
                continue;
            }
            idleStrategy.reset();
            deliver(order);
        }
    }

    /** 마감 시각까지 짧은 백오프로 재시도한다. 마감을 넘기거나 예상하지 못한 예외가 나면 기록하고 끝낸다. */
    void deliver(PendingOrder order) {
        long deadlineNanos = System.nanoTime() + deadlineMillis * 1_000_000L;
        long backoffNanos = FIRST_BACKOFF_NANOS;
        int attempts = 0;
        String lastReason = "알 수 없음";
        while (true) {
            attempts++;
            try {
                sender.send(order.side(), order.accountId(), order.stockCode(), order.price(), order.quantity(),
                    order.requestId(), order.originNanos());
                return;
            } catch (OrderPublishException e) {
                lastReason = e.getMessage();
            } catch (RuntimeException e) {
                recordFailure(order, e.toString(), attempts);
                return;
            }
            if (System.nanoTime() >= deadlineNanos) {
                break;
            }
            LockSupport.parkNanos(backoffNanos);
            backoffNanos = Math.min(backoffNanos * 2, MAX_BACKOFF_NANOS);
        }
        recordFailure(order, lastReason, attempts);
    }

    /** ERROR 로그를 먼저 남기고 Redis에 기록한다. 어떤 경우에도 예외를 밖으로 내보내지 않는다. */
    void recordFailure(PendingOrder order, String reason, int attempts) {
        log.error("[api] 주문 전달 실패: requestId={} 계좌={} 종목={} 방향={} 가격={} 수량={} 시도={} 사유={}",
            order.requestId(), order.accountId(), order.stockCode(), order.side(), order.price().toPlainString(),
            order.quantity(), attempts, reason);
        try {
            failureStore.put(order, reason, attempts, System.currentTimeMillis());
        } catch (RuntimeException e) {
            log.error("[api] 주문 전달 실패 기록 저장 실패: requestId={} 원인={}", order.requestId(), e.toString());
        }
    }

    /** 새 주문을 막고, 큐가 비기를 최대 5초 기다린 뒤, 남은 주문은 「종료 시 미전달」로 기록한다. */
    @Override
    public void close() {
        accepting = false;
        if (dispatchThread != null) {
            long end = System.nanoTime() + DRAIN_WAIT_ON_CLOSE_MILLIS * 1_000_000L;
            while (!queue.isEmpty() && dispatchThread.isAlive() && System.nanoTime() < end) {
                LockSupport.parkNanos(5_000_000L);
            }
            running = false;
            try {
                dispatchThread.join(deadlineMillis + 1_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            dispatchThread = null;
        }
        PendingOrder left;
        while ((left = queue.poll()) != null) {
            recordFailure(left, "종료 시 미전달", 0);
        }
    }
}
