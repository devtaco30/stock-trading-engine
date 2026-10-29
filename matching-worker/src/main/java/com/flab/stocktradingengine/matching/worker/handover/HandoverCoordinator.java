package com.flab.stocktradingengine.matching.worker.handover;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.IntFunction;

import com.flab.stocktradingengine.matching.worker.coordination.MovedSlotRequests;

/**
 * 호가창을 받아야 하는 종목 slot을 배정 콜백(Kafka consumer thread) 대신 전용 thread가 하나씩 이전 담당에게 요청한다. 콜백은
 * 종목 slot을 queue에 넣기만 하고 요청 전송(Publication offer)을 기다리지 않는다. 요청 중 예외(예: 풀에 없는 이전 담당 주소)는
 * 로그로 남기고 다음 종목 slot을 계속 처리한다. 맵에서 이전 담당을 못 찾았을 때 다시 시도하는 규칙은 U15c의 대기 한도가 정한다.
 */
public final class HandoverCoordinator implements MovedSlotRequests, AutoCloseable {

    private static final Logger log = System.getLogger(HandoverCoordinator.class.getName());

    private static final long CLOSE_JOIN_TIMEOUT_MILLIS = 1000L;

    private final IntFunction<Optional<Long>> requestFunction;
    private final BlockingQueue<Integer> slots = new LinkedBlockingQueue<>();

    private volatile boolean running;
    private Thread thread;

    /** @param requestFunction 종목 slot의 호가창을 요청하고, 요청을 보냈으면 이동 번호를 돌려주는 함수(실제 구현은 {@code HandoverRequester#request}) */
    public HandoverCoordinator(IntFunction<Optional<Long>> requestFunction) {
        this.requestFunction = requestFunction;
    }

    @Override
    public void requestHandover(int stockSlot) {
        slots.add(stockSlot);
    }

    public void start() {
        running = true;
        thread = new Thread(this::loop, "handover-coordinator");
        thread.setDaemon(true);
        thread.start();
    }

    private void loop() {
        while (running) {
            Integer stockSlot;
            try {
                stockSlot = slots.take();
            } catch (InterruptedException e) {
                return;
            }
            try {
                Optional<Long> transferId = requestFunction.apply(stockSlot);
                if (transferId.isEmpty()) {
                    log.log(Level.WARNING, "[호가창 이동] 요청을 보내지 못함(맵에 이동 중 항목이 없거나 전송 실패): 종목 slot=" + stockSlot);
                }
            } catch (RuntimeException e) {
                log.log(Level.ERROR, "[호가창 이동] 요청 중 예외: 종목 slot=" + stockSlot + " 원인=" + e);
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
