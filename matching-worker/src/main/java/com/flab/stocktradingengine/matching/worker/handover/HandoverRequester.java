package com.flab.stocktradingengine.matching.worker.handover;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import java.util.function.LongConsumer;

import org.agrona.concurrent.UnsafeBuffer;

import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.aeron.MatchingSlotTarget;
import com.flab.stocktradingengine.matching.worker.coordination.HandoverEndpointPool;

import io.aeron.Aeron;
import io.aeron.Publication;

/**
 * 호가창 이동을 받는 쪽(Z)이 이전 담당(X)에게 요청(스트림 2008)을 보낸다. X의 주소는 맵에 남아 있는 이전 담당 항목
 * ({@code X주소;moving})에서 알고, 응답을 받을 주소로 자기 주문 입구 주소를 요청에 담는다. 종목 slot마다 지금 진행 중인 요청의
 * 이동 번호를 기억해, 다른 번호의 응답을 버릴 수 있게 한다({@link HandoverReceiver}).
 *
 * <p>이동 번호는 서버 번호를 상위 비트에 둔 카운터다(ADR-023의 orderId와 같은 방식).</p>
 */
public final class HandoverRequester implements AutoCloseable, HandoverRequestPort {

    private static final Logger log = System.getLogger(HandoverRequester.class.getName());

    private static final long SEND_DEADLINE_NANOS = 2_000_000_000L;

    private final String ownChannel;
    private final HandoverEndpointPool pool;
    private final IntFunction<Optional<MatchingSlotTarget>> mapLookup;
    private final long nodeId;
    private final AtomicLong counter = new AtomicLong();
    private final Map<String, Publication> requestPublications = new HashMap<>();
    private final Map<Integer, Long> currentTransferBySlot = new ConcurrentHashMap<>();
    // 재요청에 쓰려고 보낸 요청의 바이트와 보낸 곳을 종목 slot별로 기억한다. 같은 요청(같은 이동 번호)을 그대로 다시 보낸다.
    private record Sent(long transferId, byte[] wire, Publication publication, String previousOwner) {
    }

    private final Map<Integer, Sent> sentBySlot = new ConcurrentHashMap<>();
    // 응답이 완성되지 않은 채 끝난 이동(취소, 새 요청으로 대체)의 이동 번호를 알린다. 응답 일부만 모아 둔 조립 상태를 지우는 데 쓴다.
    private volatile LongConsumer abandonedTransferListener = transferId -> { };

    public HandoverRequester(Aeron aeron, String ownChannel, HandoverEndpointPool pool,
                             IntFunction<Optional<MatchingSlotTarget>> mapLookup, long nodeId) {
        this.ownChannel = ownChannel;
        this.pool = pool;
        this.mapLookup = mapLookup;
        this.nodeId = nodeId;
        for (String peer : pool.peers()) {
            requestPublications.put(peer, aeron.addPublication(peer, AeronStreamIds.ORDER_BOOK_REQUEST));
        }
    }

    /**
     * 이 종목 slot의 호가창을 보내 달라고 이전 담당에게 요청한다.
     *
     * @return 보낸 요청의 이동 번호. 맵에 이동 중 항목이 없거나(첫 배정이거나 아직 맵에 도착하지 않음) 요청을 마감 안에 못 보냈으면 빈 값
     * @throws IllegalStateException 맵의 이전 담당 주소가 풀에 없을 때(설정 어긋남)
     */
    @Override
    public Optional<Long> request(int stockSlot) {
        Optional<MatchingSlotTarget> target = mapLookup.apply(stockSlot);
        if (target.isEmpty() || !target.get().moving()) {
            return Optional.empty();
        }
        String previousOwner = pool.requireInPool(target.get().endpoint());
        Publication publication = requestPublications.get(previousOwner);
        if (publication == null) {
            // 풀에는 있으나 자기 자신인 경우다. 자기 자신에게 호가창을 요청하지 않는다.
            return Optional.empty();
        }
        long transferId = (nodeId << 53) | counter.incrementAndGet();
        byte[] wire = new HandoverRequest(transferId, stockSlot, ownChannel).encode();
        Long replaced = currentTransferBySlot.put(stockSlot, transferId);
        if (replaced != null) {
            abandonedTransferListener.accept(replaced);
        }
        long deadline = System.nanoTime() + SEND_DEADLINE_NANOS;
        if (!OfferSupport.offerWithDeadline(publication, new UnsafeBuffer(wire), wire.length, deadline)) {
            currentTransferBySlot.remove(stockSlot, transferId);
            return Optional.empty();
        }
        sentBySlot.put(stockSlot, new Sent(transferId, wire, publication, previousOwner));
        log.log(Level.INFO, "[호가창 이동] 요청 보냄: 이동 번호=" + transferId + " 종목 slot=" + stockSlot + " 이전 담당=" + previousOwner);
        return Optional.of(transferId);
    }

    @Override
    public boolean hasMovingEntry(int stockSlot) {
        Optional<MatchingSlotTarget> target = mapLookup.apply(stockSlot);
        return target.isPresent() && target.get().moving();
    }

    /** 이 종목 slot에서 지금 진행 중인 요청의 이동 번호. 없으면 빈 값. */
    Optional<Long> currentTransferId(int stockSlot) {
        return Optional.ofNullable(currentTransferBySlot.get(stockSlot));
    }

    /** 진행 중인 같은 요청(같은 이동 번호)을 다시 보낸다. */
    @Override
    public boolean resend(int stockSlot) {
        Sent sent = sentBySlot.get(stockSlot);
        if (sent == null || !currentTransferBySlot.containsKey(stockSlot)
            || currentTransferBySlot.get(stockSlot) != sent.transferId()) {
            return false;
        }
        long deadline = System.nanoTime() + SEND_DEADLINE_NANOS;
        boolean offered = OfferSupport.offerWithDeadline(sent.publication(), new UnsafeBuffer(sent.wire()), sent.wire().length, deadline);
        if (offered) {
            log.log(Level.INFO, "[호가창 이동] 요청 다시 보냄: 이동 번호=" + sent.transferId() + " 종목 slot=" + stockSlot
                + " 이전 담당=" + sent.previousOwner());
        }
        return offered;
    }

    @Override
    public boolean inFlight(int stockSlot) {
        return currentTransferBySlot.containsKey(stockSlot);
    }

    /** 이 종목 slot에서 진행 중인 요청을 취소한다. 이후 도착하는 그 요청의 응답은 번호가 달라 버려진다. */
    @Override
    public void cancel(int stockSlot) {
        Long cancelled = currentTransferBySlot.remove(stockSlot);
        sentBySlot.remove(stockSlot);
        if (cancelled != null) {
            abandonedTransferListener.accept(cancelled);
        }
    }

    /** 응답이 완성되지 않은 채 끝난 이동의 이동 번호를 받을 곳을 정한다. */
    void onAbandonedTransfer(LongConsumer listener) {
        this.abandonedTransferListener = listener;
    }

    /** 적재 요청을 낼 이동을 끝난 것으로 기록한다. 그 사이 취소되었거나 다른 번호로 바뀌었으면 false이고, 이때는 적재를 요청하지 않는다. */
    boolean finish(int stockSlot, long transferId) {
        boolean wasCurrent = currentTransferBySlot.remove(stockSlot, transferId);
        if (wasCurrent) {
            sentBySlot.remove(stockSlot);
        }
        return wasCurrent;
    }

    @Override
    public void close() {
        requestPublications.values().forEach(Publication::close);
    }
}
