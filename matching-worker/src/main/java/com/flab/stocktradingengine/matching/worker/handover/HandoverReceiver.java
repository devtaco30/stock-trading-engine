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
    // 적재 요청을 낸 직후 회수가 끼었을 때 낡은 사본을 지우도록 정리를 요청하는 곳.
    private final java.util.function.IntConsumer cleanupSink;
    private final Subscription subscription;
    private final HandoverAssembler assembler = new HandoverAssembler();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong discardedMessages = new AtomicLong();
    // 같은 종목 slot의 번호 불일치 경고를 10초에 한 번으로 줄인다(근거 없는 임의 간격).
    private final RateLimitedLog mismatchLog = new RateLimitedLog(10_000L);
    private final AtomicLong messageFailures = new AtomicLong();

    private Thread thread;

    public HandoverReceiver(Aeron aeron, String ownChannel, HandoverRequester requester, HandoverLoadSink loadSink) {
        this(aeron, ownChannel, requester, loadSink, stockSlot -> { });
    }

    public HandoverReceiver(Aeron aeron, String ownChannel, HandoverRequester requester, HandoverLoadSink loadSink,
                            java.util.function.IntConsumer cleanupSink) {
        this.cleanupSink = cleanupSink;
        this.requester = requester;
        this.loadSink = loadSink;
        this.subscription = aeron.addSubscription(ownChannel, AeronStreamIds.ORDER_BOOK_RESPONSE);
        requester.onAbandonedTransfer(assembler::discard);
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

    /** 메시지 하나가 실패해도 그 메시지만 포기하고 thread는 계속 돈다. */
    private void onMessage(DirectBuffer buffer, int offset, int length, Header header) {
        try {
            handleMessage(buffer, offset, length);
        } catch (RuntimeException e) {
            messageFailures.incrementAndGet();
            log.log(Level.ERROR, "[호가창 이동] 응답 메시지 하나를 처리하지 못하고 포기함: " + e);
        }
    }

    private void handleMessage(DirectBuffer buffer, int offset, int length) {
        byte[] wire = new byte[length];
        buffer.getBytes(offset, wire);
        HandoverMessage message = HandoverMessage.decode(wire, 0, length);
        Optional<Long> current = requester.currentTransferId(message.stockSlot());
        if (current.isEmpty() || current.get() != message.transferId()) {
            discardedMessages.incrementAndGet();
            assembler.discard(message.transferId());
            int suppressed = mismatchLog.tryAcquire(message.stockSlot());
            if (suppressed >= 0) {
                log.log(Level.WARNING, "[호가창 이동] 진행 중인 요청과 이동 번호가 달라 응답을 버림: " + message
                    + " 진행 중=" + current.map(String::valueOf).orElse("없음") + (suppressed > 0 ? " (같은 로그 " + suppressed + "건을 건너뜀)" : ""));
            }
            return;
        }
        Optional<List<byte[]>> complete = assembler.accept(message);
        if (complete.isPresent()) {
            int cancelGenerationBefore = requester.cancelGeneration(message.stockSlot());
            if (!requester.finish(message.stockSlot(), message.transferId())) {
                // 번호를 확인한 뒤 완성되기 전에 취소되었거나 새 이동 번호로 바뀌었다. 담당이 아닌 종목 slot에 낡은 호가창이 올라가지 않게 적재를
                // 요청하지 않는다.
                discardedMessages.incrementAndGet();
                log.log(Level.WARNING, "[호가창 이동] 완성 직전에 취소되었거나 새 이동 번호로 바뀌어 적재를 요청하지 않음: 이동 번호=" + message.transferId()
                    + " 종목 slot=" + message.stockSlot());
                return;
            }
            log.log(Level.INFO, "[호가창 이동] 응답 완성: 이동 번호=" + message.transferId() + " 종목 slot=" + message.stockSlot()
                + " 종목 수=" + complete.get().size());
            loadSink.onAssembled(message.transferId(), message.stockSlot(), complete.get());
            if (requester.cancelGeneration(message.stockSlot()) != cancelGenerationBefore) {
                // 적재를 요청하는 동안 그 종목 slot이 회수되었다. 회수 쪽의 정리 요청이 적재 요청보다 먼저 들어갔다면 낡은 사본이 담당 아닌
                // 종목 slot에 올라가므로, 요청 queue의 순서상 적재 뒤에 들어가는 정리를 한 번 더 요청한다.
                log.log(Level.WARNING, "[호가창 이동] 적재를 요청하는 동안 회수되어 낡은 사본을 지우도록 정리를 다시 요청함: 이동 번호="
                    + message.transferId() + " 종목 slot=" + message.stockSlot());
                cleanupSink.accept(message.stockSlot());
            }
        }
    }

    /** 일부만 도착해 조립 중인 이동의 수. */
    int pendingAssemblies() {
        return assembler.pendingCount();
    }

    /** 처리 중 예외가 나 포기한 응답 메시지 수(깨진 메시지, 적재 요청 실패 등). */
    public long messageFailures() {
        return messageFailures.get();
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
