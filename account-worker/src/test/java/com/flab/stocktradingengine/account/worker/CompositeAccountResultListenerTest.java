package com.flab.stocktradingengine.account.worker;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.math.BigDecimal;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.account.disruptor.AccountResultListener;

/**
 * AccountEngine은 리스너를 하나만 받는데(AccountEngineConfig), account-worker는 로깅과
 * 정산 요청 발행을 동시에 리스너로 등록해야 한다. 콜백 하나가 delegate 전부에게 그대로 전달되는지 검증한다.
 */
class CompositeAccountResultListenerTest {

    @Test
    void 콜백_하나가_모든_delegate에게_전달된다() {
        AccountResultListener first = mock(AccountResultListener.class);
        AccountResultListener second = mock(AccountResultListener.class);
        CompositeAccountResultListener composite = new CompositeAccountResultListener(List.of(first, second));

        composite.onUnpaidRecorded(1L, 9001L, new BigDecimal("60000"));

        verify(first).onUnpaidRecorded(1L, 9001L, new BigDecimal("60000"));
        verify(second).onUnpaidRecorded(1L, 9001L, new BigDecimal("60000"));
    }
}
