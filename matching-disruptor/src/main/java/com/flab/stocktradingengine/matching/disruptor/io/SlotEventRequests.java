package com.flab.stocktradingengine.matching.disruptor.io;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;

/**
 * 종목 slot 받음·넘김 요청을 다른 thread(Kafka 배정 콜백)에서 주문 수신 thread로 넘기는 통로.
 *
 * <p>{@link MatchingEngine}의 링버퍼는 프로듀서가 하나뿐인 구성이다({@code ProducerType.SINGLE}). 주문을 넣는 thread는
 * 주문 수신 thread 하나이므로, 배정 콜백 thread가 받음·넘김 이벤트를 직접 링버퍼에 넣으면 프로듀서가 둘이 된다.
 * 그래서 콜백 thread는 요청만 이 queue에 넣고, 수신 thread가 매 반복마다 {@link #drainTo}로 꺼내 이벤트로 발행한다.
 * 같은 thread가 주문과 이벤트를 넣으므로 링버퍼 안의 순서는 수신 thread가 본 순서와 같다.</p>
 */
public final class SlotEventRequests {

    private record Request(EventType type, int stockSlot) {
    }

    private final Queue<Request> queue = new ConcurrentLinkedQueue<>();

    /** 어느 thread에서나 부를 수 있다. */
    public void requestAcquire(int stockSlot) {
        queue.add(new Request(EventType.SLOT_ACQUIRED, stockSlot));
    }

    /** 어느 thread에서나 부를 수 있다. */
    public void requestRelease(int stockSlot) {
        queue.add(new Request(EventType.SLOT_RELEASED, stockSlot));
    }

    /**
     * 쌓인 요청을 넣은 순서대로 엔진에 발행한다. 주문을 발행하는 수신 thread에서만 불러야 한다.
     *
     * @return 발행한 요청 수
     */
    public int drainTo(MatchingEngine engine) {
        int drained = 0;
        Request request;
        while ((request = queue.poll()) != null) {
            if (request.type() == EventType.SLOT_ACQUIRED) {
                engine.publishSlotAcquired(request.stockSlot());
            } else {
                engine.publishSlotReleased(request.stockSlot());
            }
            drained++;
        }
        return drained;
    }
}
