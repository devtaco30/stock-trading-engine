package com.flab.stocktradingengine.matching.worker.handover;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.matching.disruptor.handover.HandoverFragmenter;
import com.flab.stocktradingengine.matching.disruptor.handover.HandoverMessage;
import com.flab.stocktradingengine.matching.worker.coordination.HandoverEndpointPool;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.Header;

/**
 * 호가창 이동을 보내는 쪽(X)의 전용 thread. 자기 주문 입구 주소의 스트림 2008에서 요청을 받아, 보관한 종목별 바이트를
 * 요청한 쪽의 응답 주소(스트림 2009)로 종목마다 메시지 하나씩 보낸다. 매칭 thread는 offer하지 않는다 — 큰 메시지의
 * offer가 배압으로 재시도되면 같은 thread가 처리하는 다른 종목 slot의 매칭이 멈추기 때문이다(11-6절).
 *
 * <p>응답 주소가 풀에 없는 요청과 보관한 바이트가 없는 요청은 응답하지 않고 로그와 횟수로 남긴다. 요청한 쪽이 대기 한도 안에서
 * 다시 요청한다(U15c).</p>
 */
public final class HandoverResponder implements AutoCloseable {

    private static final Logger log = System.getLogger(HandoverResponder.class.getName());

    private static final int FRAGMENT_LIMIT = 10;
    private static final long OFFER_DEADLINE_NANOS = 5_000_000_000L;
    private static final long CLOSE_JOIN_TIMEOUT_MILLIS = 1000L;

    private final HandoverEndpointPool pool;
    private final HandoverBookStore store;
    private final Subscription requestSubscription;
    private final Map<String, Publication> responsePublications = new HashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    // 한 번의 poll에서 읽은 요청 중 종목 slot별로 마지막 하나. 이 thread만 쓴다.
    private final Map<Integer, HandoverRequest> collected = new java.util.LinkedHashMap<>();
    private final AtomicLong requestsAfterCleanup = new AtomicLong();
    // 종목 slot별로 마지막에 응답한 요청(응답 주소와 이동 번호). 정리한 뒤 같은 요청이 늦게 다시 오면 중복이고, 다른 요청이면 새 담당이 다시 요청한 것이라 구분한다.
    // 이동 번호는 pod마다 앞 비트가 달라 서로 크기를 비교할 수 없으므로 같은지만 본다. 이 thread만 쓴다.
    private record Answered(String replyChannel, long transferId) {
    }

    private final Map<Integer, Answered> lastAnswered = new HashMap<>();
    // 같은 종목 slot의 보관 없음 로그를 10초에 한 번으로 줄인다(근거 없는 임의 간격). 카운터는 건너뛴 로그와 무관하게 센다.
    private final RateLimitedLog noBookLog = new RateLimitedLog(10_000L);
    private final RateLimitedLog afterCleanupLog = new RateLimitedLog(10_000L);
    private final AtomicLong requestsWithoutBook = new AtomicLong();
    private final AtomicLong requestsRejectedByPool = new AtomicLong();
    private final AtomicLong responsesAbandoned = new AtomicLong();
    private final AtomicLong requestFailures = new AtomicLong();

    private Thread thread;

    public HandoverResponder(Aeron aeron, String ownChannel, HandoverEndpointPool pool, HandoverBookStore store) {
        this.pool = pool;
        this.store = store;
        this.requestSubscription = aeron.addSubscription(ownChannel, AeronStreamIds.ORDER_BOOK_REQUEST);
        for (String peer : pool.peers()) {
            responsePublications.put(peer, aeron.addPublication(peer, AeronStreamIds.ORDER_BOOK_RESPONSE));
        }
    }

    public void start() {
        running.set(true);
        thread = new Thread(this::pollLoop, "handover-responder");
        thread.setDaemon(true);
        thread.start();
    }

    /** 패키지 가시성 — 시험이 응답 thread를 시작하지 않고 poll을 한 번 부른다. */
    int pollOnce() {
        int fragments = requestSubscription.poll(this::collectRequest, FRAGMENT_LIMIT);
        respondToCollected();
        return fragments;
    }

    private void pollLoop() {
        IdleStrategy idle = new BackoffIdleStrategy();
        while (running.get()) {
            idle.idle(pollOnce());
        }
    }

    /** 한 번의 poll에서 읽은 요청을 종목 slot별로 마지막 하나만 남긴다. 요청 하나가 깨져도 그 요청만 포기한다. */
    private void collectRequest(DirectBuffer buffer, int offset, int length, Header header) {
        try {
            byte[] wire = new byte[length];
            buffer.getBytes(offset, wire);
            HandoverRequest request = HandoverRequest.decode(wire, 0, length);
            collected.put(request.stockSlot(), request);
        } catch (RuntimeException e) {
            requestFailures.incrementAndGet();
            log.log(Level.ERROR, "[호가창 이동] 요청 하나를 처리하지 못하고 포기함: " + e);
        }
    }

    private void respondToCollected() {
        if (collected.isEmpty()) {
            return;
        }
        List<HandoverRequest> batch = new java.util.ArrayList<>(collected.values());
        collected.clear();
        for (HandoverRequest request : batch) {
            try {
                handleRequest(request);
            } catch (RuntimeException e) {
                requestFailures.incrementAndGet();
                log.log(Level.ERROR, "[호가창 이동] 요청 하나를 처리하지 못하고 포기함: " + e);
            }
        }
    }

    private void handleRequest(HandoverRequest request) {
        Publication reply = responsePublications.get(request.replyChannel());
        if (reply == null) {
            requestsRejectedByPool.incrementAndGet();
            log.log(Level.ERROR, "[호가창 이동] 응답 주소가 풀에 없어 요청을 버림: " + request);
            return;
        }
        Optional<List<byte[]>> stored = store.get(request.stockSlot());
        if (stored.isEmpty()) {
            if (store.wasRemoved(request.stockSlot())
                && new Answered(request.replyChannel(), request.transferId()).equals(lastAnswered.get(request.stockSlot()))) {
                requestsAfterCleanup.incrementAndGet();
                int suppressed = afterCleanupLog.tryAcquire(request.stockSlot());
                String suffix = suppressed > 0 ? " (같은 로그 " + suppressed + "건을 건너뜀)" : "";
                if (suppressed >= 0) {
                    log.log(Level.INFO, "[호가창 이동] 정리한 뒤 늦게 도착한 요청이라 응답하지 않음: " + request + suffix);
                }
            } else {
                requestsWithoutBook.incrementAndGet();
                int suppressed = noBookLog.tryAcquire(request.stockSlot());
                String suffix = suppressed > 0 ? " (같은 로그 " + suppressed + "건을 건너뜀)" : "";
                if (suppressed >= 0) {
                    String why = store.wasRemoved(request.stockSlot()) ? "정리한 뒤 새로 도착한 요청이라" : "보관한 호가창이 없어";
                    log.log(Level.WARNING, "[호가창 이동] " + why + " 응답하지 못함: " + request + suffix);
                }
            }
            return;
        }
        lastAnswered.put(request.stockSlot(), new Answered(request.replyChannel(), request.transferId()));
        send(reply, request, stored.get());
    }

    private void send(Publication reply, HandoverRequest request, List<byte[]> bodies) {
        List<HandoverMessage> messages = HandoverFragmenter.split(
            request.transferId(), request.stockSlot(), bodies, reply.maxMessageLength());
        long deadline = System.nanoTime() + OFFER_DEADLINE_NANOS;
        for (HandoverMessage message : messages) {
            byte[] wire = message.encode();
            if (!OfferSupport.offerWithDeadline(reply, new UnsafeBuffer(wire), wire.length, deadline)) {
                responsesAbandoned.incrementAndGet();
                log.log(Level.ERROR, "[호가창 이동] 응답을 마감 안에 보내지 못함: " + request + " 보낸 순번=" + message.index());
                return;
            }
        }
    }

    /** 정리한 뒤 늦게 도착해 응답하지 않은 중복 요청 수(이미 응답한 요청과 응답 주소·이동 번호가 같은 요청). */
    public long requestsAfterCleanup() {
        return requestsAfterCleanup.get();
    }

    /** 요청은 왔지만 한 번도 보관한 적 없어 응답하지 못한 횟수. */
    public long requestsWithoutBook() {
        return requestsWithoutBook.get();
    }

    /** 응답 주소가 풀에 없어 버린 요청 횟수. */
    public long requestsRejectedByPool() {
        return requestsRejectedByPool.get();
    }

    /** 처리 중 예외가 나 포기한 요청 횟수(깨진 요청, 닫힌 Publication 등). */
    public long requestFailures() {
        return requestFailures.get();
    }

    /** 마감 안에 보내지 못해 포기한 응답 횟수. */
    public long responsesAbandoned() {
        return responsesAbandoned.get();
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
        requestSubscription.close();
        responsePublications.values().forEach(Publication::close);
    }
}
