package com.flab.stocktradingengine.account.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.account.disruptor.domain.RejectReason;
import com.flab.stocktradingengine.account.worker.listener.MetricsAccountResultListener;

/**
 * C7 부하 테스트 하네스 — {@link MetricsAccountResultListener}가 콜백 호출 수를 카운터에
 * 정확히 반영하는지 검증한다. 리포터 스레드(1초 주기 로그)는 여기서 안 본다 — 핫패스 카운터
 * 증가 로직만 단위검증.
 */
class MetricsAccountResultListenerTest {

    @Test
    @DisplayName("콜백이 오면 해당 카운터가 늘어난다")
    void 콜백_호출_수가_카운터에_반영된다() {
        MetricsAccountResultListener listener = new MetricsAccountResultListener();

        listener.onAccepted(1L, 10L, "r1", new BigDecimal("40000"));
        listener.onSellAccepted(1L, 11L, "r2", 5);
        listener.onRejected(1L, 0L, "r3", RejectReason.INSUFFICIENT);
        listener.onFillApplied(1L, 10L, 9001L, true);
        listener.onFillApplied(1L, 10L, 9002L, false); // 이미 반영한 tradeId 재도착(멱등 무시) — 카운트 안 됨
        listener.onDuplicateRequest(1L, "r1");

        MetricsAccountResultListener.Counts counts = listener.counts();
        assertEquals(1L, counts.accepted());
        assertEquals(1L, counts.sellAccepted());
        assertEquals(1L, counts.rejected());
        assertEquals(1L, counts.fillApplied());
        assertEquals(1L, counts.duplicate());
    }
}
