package com.flab.stocktradingengine.account.worker.messaging;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Map;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.springframework.stereotype.Component;

import com.flab.stocktradingengine.account.disruptor.domain.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.domain.RejectReason;
import com.flab.stocktradingengine.codec.OrderResultCodec;
import com.flab.stocktradingengine.codec.OrderResultEntry;
import com.flab.stocktradingengine.codec.OrderVerdict;

import io.aeron.ExclusivePublication;
import io.aeron.Publication;

/**
 * 주문 접수 판정(수락·거부·중복)을 Aeron Archive 결과 스트림에 기록하는 {@link AccountResultListener}
 * 구현체. 판정만 담고 체결·정산·상태변경 콜백은 no-op이다.
 *
 * <h3>offer 실패 정책 — {@code AeronArchiveAccountJournal}(저널)과 같은 정책을 따른다</h3>
 * <p>이 콜백은 계좌 엔진 단일 스레드에서만 불린다. 저널과 마찬가지로 {@code offer}가 성공(반환값 ≥ 0)할
 * 때까지 {@link BackoffIdleStrategy}로 재시도하고, {@code CLOSED}·{@code MAX_POSITION_EXCEEDED}는
 * 스트림이 복구 불가 상태라는 뜻이라 예외를 던져 계좌 워커 소비자를 fail-fast로 멈춘다
 * ({@code AccountExceptionHandler}가 받는다). 이 스트림은 로컬 IPC라 Kafka와 달리 지연이 사실상
 * 없어, "핫패스를 막지 않는다"는 결정(LLD)과 충돌하지 않는다 — 느려지는 대상은 Kafka뿐이다.</p>
 */
@Component
public class OrderResultRecorder implements AccountResultListener {

    private static final int ENCODE_BUFFER_SIZE = 256;

    private final ExclusivePublication orderResultPublication;
    private final OrderResultCodec codec = new OrderResultCodec();
    private final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(ENCODE_BUFFER_SIZE));
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();

    public OrderResultRecorder(ExclusivePublication orderResultPublication) {
        this.orderResultPublication = orderResultPublication;
    }

    @Override
    public void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin) {
        record(accountId, orderId, requestId, OrderVerdict.ACCEPTED, null);
    }

    @Override
    public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
        record(accountId, orderId, requestId, OrderVerdict.ACCEPTED, null);
    }

    @Override
    public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
        record(accountId, orderId, requestId, OrderVerdict.REJECTED, reason.name());
    }

    @Override
    public void onDuplicateRequest(long accountId, String requestId) {
        record(accountId, 0L, requestId, OrderVerdict.DUPLICATE, null);
    }

    @Override
    public void onFillApplied(long accountId, long orderId, long tradeId, boolean applied) {
    }

    @Override
    public void onSettlementApplied(long accountId, long settlementRef, boolean applied) {
    }

    @Override
    public void onUnpaidRecorded(long accountId, long tradeId, BigDecimal amount) {
    }

    @Override
    public void onStateChanged(long accountId, BigDecimal balance, Map<String, Integer> holdings, long seq) {
    }

    private void record(long accountId, long orderId, String requestId, OrderVerdict verdict, String rejectReason) {
        OrderResultEntry entry =
            new OrderResultEntry(accountId, orderId, requestId, verdict, rejectReason, System.currentTimeMillis());
        int length = codec.encode(buffer, 0, entry);

        idleStrategy.reset();
        long result;
        while ((result = orderResultPublication.offer(buffer, 0, length)) < 0) {
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException(
                    "주문 결과 기록 스트림이 복구 불가 상태입니다(offer 결과=" + result + "): 계좌 워커 소비자를 멈춥니다");
            }
            idleStrategy.idle();
        }
    }
}
