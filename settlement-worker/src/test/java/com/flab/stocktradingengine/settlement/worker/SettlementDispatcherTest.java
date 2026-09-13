package com.flab.stocktradingengine.settlement.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.settlement.worker.entity.PendingSettlement;
import com.flab.stocktradingengine.settlement.worker.entity.SettlementStatus;
import com.flab.stocktradingengine.settlement.worker.repository.PendingSettlementRepository;
import com.flab.stocktradingengine.settlement.worker.service.PendingSettlementSettler;
import com.flab.stocktradingengine.settlement.worker.service.SettlementDispatcher;

/**
 * 스캔(조회)과 건별 발행+정산완료(PendingSettlementSettler)를 분리한 오케스트레이터를 검증한다.
 * 한 건의 실패가 다른 건의 처리를 막지 않는지가 핵심이다(배치 트랜잭션으로 묶지 않는 이유).
 */
class SettlementDispatcherTest {

    @Test
    void 만기_도래한_PENDING을_전부_찾아_건별로_settle을_호출한다() {
        PendingSettlementRepository repository = mock(PendingSettlementRepository.class);
        PendingSettlementSettler settler = mock(PendingSettlementSettler.class);
        when(repository.findByStatusAndDueAtEpochMillisLessThanEqual(SettlementStatus.PENDING, 1000L))
            .thenReturn(List.of(
                new PendingSettlement(9001L, 1L, new BigDecimal("60000"), 900L),
                new PendingSettlement(9002L, 2L, new BigDecimal("30000"), 950L)
            ));
        SettlementDispatcher dispatcher = new SettlementDispatcher(repository, settler);

        int dispatched = dispatcher.dispatchDue(1000L);

        assertThat(dispatched).isEqualTo(2);
        verify(settler).settle(9001L);
        verify(settler).settle(9002L);
    }

    @Test
    void 한_건이_실패해도_나머지_건은_계속_처리한다() {
        PendingSettlementRepository repository = mock(PendingSettlementRepository.class);
        PendingSettlementSettler settler = mock(PendingSettlementSettler.class);
        when(repository.findByStatusAndDueAtEpochMillisLessThanEqual(SettlementStatus.PENDING, 1000L))
            .thenReturn(List.of(
                new PendingSettlement(9001L, 1L, new BigDecimal("60000"), 900L),
                new PendingSettlement(9002L, 2L, new BigDecimal("30000"), 950L)
            ));
        doThrow(new IllegalStateException("broker down")).when(settler).settle(9001L);
        SettlementDispatcher dispatcher = new SettlementDispatcher(repository, settler);

        int dispatched = dispatcher.dispatchDue(1000L);

        assertThat(dispatched).isEqualTo(1); // 9001은 실패해 PENDING 유지, 9002만 성공
        verify(settler).settle(9002L);
    }
}
