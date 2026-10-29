package com.flab.stocktradingengine.matching.worker.handover;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
 * 호가창으로 자동 시작하지 않는다. 포기 뒤에도 정한 간격마다 새 이동 번호로 다시 요청한다.</li>
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
    // 포기한 뒤 다시 요청하기까지의 간격. 0 이하이면 포기 뒤 다시 요청하지 않는다.
    private final long abandonedRetryIntervalNanos;
    // 재요청 직전에 그 종목 slot이 아직 호가창 적재를 기다리는지 확인한다. 정하지 않으면 항상 기다리는 것으로 본다.
    private volatile java.util.function.IntPredicate stillAwaitingLoad = slot -> true;
    /** 요청과 취소를 넣은 순서대로 한 queue에 둔다. 따로 두면 아직 시작하지 않은 요청이 그 뒤의 취소보다 먼저 처리되어 상태가 되살아난다. */
    private record Command(boolean cancel, int stockSlot) {
    }

    private final BlockingQueue<Command> commands = new LinkedBlockingQueue<>();
    private final Map<Integer, Tracked> tracked = new HashMap<>();
    // 포기한 종목 slot과 다시 요청할 시각. tracked와 겹치지 않는다. 전용 thread만 쓴다.
    private final Map<Integer, Long> abandoned = new HashMap<>();
    private final AtomicLong failures = new AtomicLong();

    private volatile boolean running;
    private Thread thread;

    /**
     * @param retryIntervalMillis 응답이 오지 않을 때 같은 요청을 다시 보내는 간격(Jack이 정한 값 1초)
     * @param responseTimeoutMillis 처음 요청부터 응답이 완성되기를 기다리는 한도(Jack이 정한 값 5초)
     */
    public HandoverCoordinator(HandoverRequestPort port, long retryIntervalMillis, long responseTimeoutMillis) {
        this(port, retryIntervalMillis, responseTimeoutMillis, 0L);
    }

    public HandoverCoordinator(HandoverRequestPort port, long retryIntervalMillis, long responseTimeoutMillis,
                               long abandonedRetryIntervalMillis) {
        this.abandonedRetryIntervalNanos = TimeUnit.MILLISECONDS.toNanos(abandonedRetryIntervalMillis);
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
        commands.add(new Command(false, stockSlot));
    }

    @Override
    public void cancelHandover(int stockSlot) {
        commands.add(new Command(true, stockSlot));
        port.cancel(stockSlot); // 진행 중 응답이 즉시 버려지게, 추적 정리는 전용 thread가 한다
    }

    /** 재요청 직전에 그 종목 slot이 아직 호가창 적재를 기다리는지 알려 주는 곳을 정한다. */
    public void setStillAwaitingLoad(java.util.function.IntPredicate stillAwaitingLoad) {
        this.stillAwaitingLoad = stillAwaitingLoad;
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
                Command first = commands.poll(TICK_MILLIS, TimeUnit.MILLISECONDS);
                if (first != null) {
                    List<Command> batch = new ArrayList<>();
                    batch.add(first);
                    commands.drainTo(batch);
                    applyCommands(batch);
                }
                tick();
                retryAbandoned();
            } catch (InterruptedException e) {
                return;
            } catch (RuntimeException e) {
                log.log(Level.ERROR, "[호가창 이동] 요청 추적 중 예외(계속 진행함): " + e);
            }
        }
    }

    /** 모인 명령을 넣은 순서대로 처리한다. 그 뒤에 같은 종목 slot의 취소가 있는 요청은 보내지 않는다. */
    private void applyCommands(List<Command> batch) {
        for (int i = 0; i < batch.size(); i++) {
            Command command = batch.get(i);
            if (command.cancel()) {
                tracked.remove(command.stockSlot());
                abandoned.remove(command.stockSlot());
                port.cancel(command.stockSlot()); // 앞선 요청이 되살린 상태까지 지운다
                continue;
            }
            boolean cancelledLater = false;
            for (int j = i + 1; j < batch.size(); j++) {
                if (batch.get(j).cancel() && batch.get(j).stockSlot() == command.stockSlot()) {
                    cancelledLater = true;
                    break;
                }
            }
            if (!cancelledLater) {
                abandoned.remove(command.stockSlot());
                startTracking(command.stockSlot());
            }
        }
    }

    private void startTracking(int stockSlot) {
        long now = System.nanoTime();
        Tracked entry = new Tracked(now);
        try {
            entry.sent = port.request(stockSlot).isPresent();
        } catch (RuntimeException e) {
            // 추적에 넣지 않고 끝내면 재요청도 포기 처리도 없이 그 slot이 영영 담당 없이 남는다. 보내지 못한 것으로 두고 추적한다.
            log.log(Level.ERROR, "[호가창 이동] 요청 중 예외(간격을 두고 다시 시도함): 종목 slot=" + stockSlot + " 원인=" + e);
        }
        if (!entry.sent) {
            log.log(Level.WARNING, "[호가창 이동] 요청을 보내지 못함(맵에 이동 중 항목이 없거나 전송 실패) — 간격을 두고 다시 시도: 종목 slot=" + stockSlot);
        }
        tracked.put(stockSlot, entry);
    }

    /** 포기한 종목 slot 중 다시 요청할 시각이 된 것을 새 이동 번호로 요청한다. */
    private void retryAbandoned() {
        if (abandoned.isEmpty()) {
            return;
        }
        long now = System.nanoTime();
        List<Integer> due = new ArrayList<>();
        for (Map.Entry<Integer, Long> entry : abandoned.entrySet()) {
            if (now - entry.getValue() >= 0) {
                due.add(entry.getKey());
            }
        }
        for (int stockSlot : due) {
            abandoned.remove(stockSlot);
            if (tracked.containsKey(stockSlot)) {
                continue; // 그 사이 새 배정으로 이미 요청이 진행 중이다.
            }
            try {
                if (!stillAwaitingLoad.test(stockSlot)) {
                    log.log(Level.INFO, "[호가창 이동] 호가창 적재를 더 기다리지 않는 종목 slot이라 다시 요청하지 않음: 종목 slot=" + stockSlot);
                    continue;
                }
                if (!port.hasMovingEntry(stockSlot)) {
                    log.log(Level.ERROR, "[호가창 이동] 맵에 이전 담당의 이동 중 항목이 없어 다시 요청하지 않음 — 종목 slot의 담당이 켜지지 않은 채 남아 있을 수 있음: 종목 slot="
                        + stockSlot);
                    continue;
                }
                startTracking(stockSlot);
            } catch (RuntimeException e) {
                log.log(Level.ERROR, "[호가창 이동] 포기 뒤 재요청 중 예외(다음 간격에 다시 시도함): 종목 slot=" + stockSlot + " 원인=" + e);
                abandoned.put(stockSlot, System.nanoTime() + abandonedRetryIntervalNanos);
            }
        }
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
                    if (abandonedRetryIntervalNanos > 0) {
                        abandoned.put(stockSlot, now + abandonedRetryIntervalNanos);
                    }
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
