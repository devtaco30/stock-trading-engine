package com.flab.stocktradingengine.matching.worker.handover;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import io.aeron.Publication;

/** 호가창 이동 thread들이 쓰는 offer 재시도. 일시적 실패는 마감 시각까지 재시도하고, 복구 불가 상태면 바로 예외를 던진다. */
final class OfferSupport {

    private OfferSupport() {
    }

    /**
     * @return 마감 안에 offer가 성공하면 true, 연결되지 않았거나 배압이 풀리지 않아 마감을 넘기면 false
     * @throws IllegalStateException Publication이 닫혔거나 위치 한도를 넘은 복구 불가 상태일 때
     */
    static boolean offerWithDeadline(Publication publication, DirectBuffer buffer, int length, long deadlineNanos) {
        IdleStrategy idle = new BackoffIdleStrategy();
        while (true) {
            long result = publication.offer(buffer, 0, length);
            if (result > 0) {
                return true;
            }
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("호가창 이동 Publication이 복구 불가 상태다: offer 결과=" + result);
            }
            if (System.nanoTime() > deadlineNanos) {
                return false;
            }
            idle.idle();
        }
    }
}
