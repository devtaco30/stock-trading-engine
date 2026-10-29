package com.flab.stocktradingengine.matching.worker.handover;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;

import com.flab.stocktradingengine.matching.worker.coordination.MovedSlotRequests;

/**
 * 호가창을 받아야 하는 종목 slot을 배정 콜백(Kafka consumer thread) 대신 전용 thread가 이전 담당에게 요청하고, 응답이 올 때까지
 * 재요청하며, 한도를 넘기면 실패로 끝낸다(U15c).
 *
 * <ul>
 * <li>처음 요청부터 {@code retryIntervalMillis}마다, 응답이 완성되지 않았으면 같은 요청(같은 이동 번호)을 다시 보낸다. 맵에서
 * 이전 담당의 이동 중 항목을 아직 못 찾아 요청을 못 보냈으면 같은 간격으로 요청을 다시 시도한다.</li>
 * <li>처음 시도부터 {@code responseTimeoutMillis}가 지나도 응답이 완성되지 않으면 실패로 본다: ERROR 로그를 남기고 진행 중 요청을
 * 취소한다(그 뒤 늦게 오는 응답은 버려진다). 그 종목 slot은 담당을 켜지 않은 채 두고, 맵은 이전 담당의 이동 중 값 그대로이며, 빈
 * 호가창으로 자동 시작하지 않는다.</li>
 * <li>배정 콜백은 종목 slot을 queue에 넣기만 한다. 요청 중 예외는 로그로 남기고 다음 종목 slot을 계속 처리한다.</li>
 * </ul>
 */
public final class HandoverCoordinator implements MovedSlotRequests, AutoCloseable {

    private static final Logger log = System.getLogger(HandoverCoordinator.class.getName());

    private static final long CLOSE_JOIN_TIMEOUT_MILLIS = 1000L;
    // 진행 중인 요청을 살피는 주기. 재요청 간격보다 충분히 짧아야 간격이 어긋나지 않는다.
    private static final long TICK_MILLIS = 10L;

    /** 종목 slot 하나의 진행 중 요청 추적. 전용 thread만 쓴다. */
    private static final class Tracked {
        final long startedAtNanos;
        long lastSentAtNanos;
        boolean sent;

        Tracked(long now) {
            this.startedAtNanos = now;
            this.lastSentAtNanos = now;
        }
    }

    private final HandoverRequestPort port;
    private final long retryIntervalNanos;
    private final long responseTimeoutNanos;
    private final BlockingQueue<Integer> newSlots = new LinkedBlockingQueue<>();
    private final BlockingQueue<Integer> cancelledSlots = new LinkedBlockingQueue<>();
    private final Map<Integer, Tracked> tracked = new HashMap<>();
    private final AtomicLong failures = new AtomicLong();

    private volatile boolean running;
    private Thread thread;

    /**
     * @param retryIntervalMillis 응답이 오지 않을 때 같은 요청을 다시 보내는 간격(Jack이 정한 값 1초)
     * @param responseTimeoutMillis 처음 요청부터 응답이 완성되기를 기다리는 한도(Jack이 정한 값 5초)
     */
    public HandoverCoordinator(HandoverRequestPort port, long retryIntervalMillis, long responseTimeoutMillis) {
        this.port = port;
        this.retryIntervalNanos = TimeUnit.MILLISECONDS.toNanos(retryIntervalMillis);
        this.responseTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(responseTimeoutMillis);
    }

    /** 재요청 없이 한 번만 요청하는 구성(시험·단순 사용): 보낸 뒤에는 추적하지 않는다. */
    public HandoverCoordinator(IntFunction<Optional<Long>> requestFunction) {
        this(requestFunction, stockSlot -> { });
    }

    public HandoverCoordinator(IntFunction<Optional<Long>> requestFunction, java.util.function.IntConsumer cancelFunction) {
        this(new HandoverRequestPort() {
            @Override
            public Optional<Long> request(int stockSlot) {
                return requestFunction.apply(stockSlot);
            }

            @Override
            public boolean resend(int stockSlot) {
                return false;
            }

            @Override
            public boolean inFlight(int stockSlot) {
                return false;
            }

            @Override
            public void cancel(int stockSlot) {
                cancelFunction.accept(stockSlot);
            }
        }, 1_000L, 5_000L);
    }

    @Override
    public void requestHandover(int stockSlot) {
        newSlots.add(stockSlot);
    }

    @Override
    public void cancelHandover(int stockSlot) {
        cancelledSlots.add(stockSlot);
        port.cancel(stockSlot); // 진행 중 응답이 즉시 버려지게, 추적 정리는 전용 thread가 한다
    }

    /** 응답을 기다리다 포기 시간을 넘겨 실패로 끝난 이동의 수. */
    public long failures() {
        return failures.get();
    }

    public void start() {
        running = true;
        thread = new Thread(this::loop, "handover-coordinator");
        thread.setDaemon(true);
        thread.start();
    }

    private void loop() {
        while (running) {
            try {
                Integer stockSlot = newSlots.poll(TICK_MILLIS, TimeUnit.MILLISECONDS);
                if (stockSlot != null) {
                    startTracking(stockSlot);
                }
                Integer cancelled;
                while ((cancelled = cancelledSlots.poll()) != null) {
                    tracked.remove(cancelled);
                }
                tick();
            } catch (InterruptedException e) {
                return;
            } catch (RuntimeException e) {
                log.log(Level.ERROR, "[호가창 이동] 요청 추적 중 예외(계속 진행함): " + e);
            }
        }
    }

    private void startTracking(int stockSlot) {
        long now = System.nanoTime();
        Tracked entry = new Tracked(now);
        try {
            entry.sent = port.request(stockSlot).isPresent();
        } catch (RuntimeException e) {
            log.log(Level.ERROR, "[호가창 이동] 요청 중 예외: 종목 slot=" + stockSlot + " 원인=" + e);
            return;
        }
        if (!entry.sent) {
            log.log(Level.WARNING, "[호가창 이동] 요청을 보내지 못함(맵에 이동 중 항목이 없거나 전송 실패) — 간격을 두고 다시 시도: 종목 slot=" + stockSlot);
        }
        tracked.put(stockSlot, entry);
    }

    /** 추적 중인 종목 slot마다 끝났는지, 포기 시간을 넘겼는지, 재요청할 때인지 본다. */
    private void tick() {
        long now = System.nanoTime();
        Iterator<Map.Entry<Integer, Tracked>> iterator = tracked.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Integer, Tracked> mapEntry = iterator.next();
            int stockSlot = mapEntry.getKey();
            Tracked entry = mapEntry.getValue();
            try {
                if (entry.sent && !port.inFlight(stockSlot)) {
                    iterator.remove(); // 응답이 완성되어 끝났거나 취소되었다.
                    continue;
                }
                if (now - entry.startedAtNanos >= responseTimeoutNanos) {
                    failures.incrementAndGet();
                    port.cancel(stockSlot);
                    iterator.remove();
                    log.log(Level.ERROR, "[호가창 이동] 응답을 " + TimeUnit.NANOSECONDS.toMillis(responseTimeoutNanos)
                        + "ms 안에 완성하지 못해 이동을 실패로 끝냄 — 담당을 켜지 않은 채 두고 맵은 이전 담당의 이동 중 값 그대로이며 빈 호가창으로 시작하지 않는다: 종목 slot="
                        + stockSlot);
                    continue;
                }
                if (now - entry.lastSentAtNanos >= retryIntervalNanos) {
                    entry.lastSentAtNanos = now;
                    if (entry.sent) {
                        port.resend(stockSlot);
                    } else {
                        entry.sent = port.request(stockSlot).isPresent();
                    }
                }
            } catch (RuntimeException e) {
                log.log(Level.ERROR, "[호가창 이동] 재요청 중 예외: 종목 slot=" + stockSlot + " 원인=" + e);
            }
        }
    }

    @Override
    public void close() {
        running = false;
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(CLOSE_JOIN_TIMEOUT_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
