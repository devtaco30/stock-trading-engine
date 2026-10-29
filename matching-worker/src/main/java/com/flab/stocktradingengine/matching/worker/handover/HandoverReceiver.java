package com.flab.stocktradingengine.matching.worker.handover;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.matching.disruptor.handover.HandoverAssembler;
import com.flab.stocktradingengine.matching.disruptor.handover.HandoverMessage;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.Header;

/**
 * 호가창 이동을 받는 쪽(Z)의 전용 수신 thread. 자기 주문 입구 주소의 스트림 2009를 {@link FragmentAssembler}로 감싸 poll한다
 * (응답 메시지가 Aeron 조각 하나의 페이로드를 넘을 수 있다, 11-6절). 주문 수신 thread와 따로 돌아, 호가창 조립이 주문 수신을
 * 멈추게 하지 않는다. 지금 진행 중인 요청의 이동 번호와 다른 응답은 버리고 그 번호의 진행 상태를 지운다. 종목이 모두 모이면
 * {@link HandoverLoadSink}로 적재를 요청한다.
 */
public final class HandoverReceiver implements AutoCloseable {

    private static final Logger log = System.getLogger(HandoverReceiver.class.getName());

    private static final int FRAGMENT_LIMIT = 10;
    private static final long CLOSE_JOIN_TIMEOUT_MILLIS = 1000L;

    private final HandoverRequester requester;
    private final HandoverLoadSink loadSink;
    private final Subscription subscription;
    private final HandoverAssembler assembler = new HandoverAssembler();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong discardedMessages = new AtomicLong();

    private Thread thread;

    public HandoverReceiver(Aeron aeron, String ownChannel, HandoverRequester requester, HandoverLoadSink loadSink) {
        this.requester = requester;
        this.loadSink = loadSink;
        this.subscription = aeron.addSubscription(ownChannel, AeronStreamIds.ORDER_BOOK_RESPONSE);
    }

    public void start() {
        running.set(true);
        thread = new Thread(this::pollLoop, "handover-receiver");
        thread.setDaemon(true);
        thread.start();
    }

    private void pollLoop() {
        IdleStrategy idle = new BackoffIdleStrategy();
        FragmentAssembler fragmentAssembler = new FragmentAssembler(this::onMessage);
        while (running.get()) {
            int fragments = subscription.poll(fragmentAssembler, FRAGMENT_LIMIT);
            idle.idle(fragments);
        }
    }

    private void onMessage(DirectBuffer buffer, int offset, int length, Header header) {
        byte[] wire = new byte[length];
        buffer.getBytes(offset, wire);
        HandoverMessage message = HandoverMessage.decode(wire, 0, length);
        Optional<Long> current = requester.currentTransferId(message.stockSlot());
        if (current.isEmpty() || current.get() != message.transferId()) {
            discardedMessages.incrementAndGet();
            assembler.discard(message.transferId());
            log.log(Level.WARNING, "[호가창 이동] 진행 중인 요청과 이동 번호가 달라 응답을 버림: " + message
                + " 진행 중=" + current.map(String::valueOf).orElse("없음"));
            return;
        }
        Optional<List<byte[]>> complete = assembler.accept(message);
        if (complete.isPresent()) {
            requester.finish(message.stockSlot(), message.transferId());
            loadSink.onAssembled(message.transferId(), message.stockSlot(), complete.get());
        }
    }

    /** 진행 중인 요청과 번호가 달라 버린 응답 메시지 수. */
    public long discardedMessages() {
        return discardedMessages.get();
    }

    @Override
    public void close() {
        running.set(false);
        if (thread != null) {
            try {
                thread.join(CLOSE_JOIN_TIMEOUT_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        subscription.close();
    }
}
