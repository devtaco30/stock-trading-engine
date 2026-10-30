package com.flab.stocktradingengine.account.disruptor.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.account.disruptor.domain.NoOpAccountResultListener;

/** 12-3절 2번 표의 3단계: 계좌 ring buffer의 남은 칸 수를 읽을 수 있다. */
class AccountEngineRingBufferMetricsTest {

    private static final int BUFFER_SIZE = 1024;
    private static final long NODE_ID = 1L;

    @Test
    void 시작하기_전에는_알_수_없어_음수이고_시작한_뒤_처리할_주문이_없으면_전체_칸이_비어_있다() {
        AccountEngine engine = new AccountEngine(BUFFER_SIZE, NODE_ID,
            (orderId, accountId, stockCode, side, price, quantity) -> { }, NoOpAccountResultListener.INSTANCE);
        assertEquals(-1L, engine.ringBufferRemainingCapacity());

        engine.start();
        try {
            assertEquals((long) BUFFER_SIZE, engine.ringBufferRemainingCapacity());
        } finally {
            engine.shutdown();
        }
    }
}
