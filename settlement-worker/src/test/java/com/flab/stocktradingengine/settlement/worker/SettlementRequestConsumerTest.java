package com.flab.stocktradingengine.settlement.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.support.Acknowledgment;

import com.flab.stocktradingengine.kafka.event.SettlementRequestEvent;

/**
 * settlement-requests 이벤트를 받아 PENDING 행으로 저장하는지, 같은 settlementRef 재도착은
 * 저장하지 않고 무시(멱등)하는지 검증한다.
 */
class SettlementRequestConsumerTest {

    @Test
    void 새_정산요청이면_PENDING으로_저장하고_커밋한다() {
        PendingSettlementRepository repository = mock(PendingSettlementRepository.class);
        when(repository.existsById(9001L)).thenReturn(false);
        Acknowledgment ack = mock(Acknowledgment.class);
        SettlementRequestConsumer consumer = new SettlementRequestConsumer(repository);

        SettlementRequestEvent event = new SettlementRequestEvent(9001L, 1L, new BigDecimal("60000"), 123L);
        consumer.consume(event, ack);

        ArgumentCaptor<PendingSettlement> saved = ArgumentCaptor.forClass(PendingSettlement.class);
        verify(repository).save(saved.capture());
        PendingSettlement pendingSettlement = saved.getValue();
        assertThat(pendingSettlement.getSettlementRef()).isEqualTo(9001L);
        assertThat(pendingSettlement.getAccountId()).isEqualTo(1L);
        assertThat(pendingSettlement.getAmount()).isEqualByComparingTo(new BigDecimal("60000"));
        assertThat(pendingSettlement.getDueAtEpochMillis()).isEqualTo(123L);
        assertThat(pendingSettlement.getStatus()).isEqualTo(SettlementStatus.PENDING);
        verify(ack).acknowledge();
    }

    @Test
    void 같은_settlementRef가_재도착하면_저장하지_않고_커밋만_한다() {
        PendingSettlementRepository repository = mock(PendingSettlementRepository.class);
        when(repository.existsById(9001L)).thenReturn(true);
        Acknowledgment ack = mock(Acknowledgment.class);
        SettlementRequestConsumer consumer = new SettlementRequestConsumer(repository);

        SettlementRequestEvent event = new SettlementRequestEvent(9001L, 1L, new BigDecimal("60000"), 123L);
        consumer.consume(event, ack);

        verify(repository, never()).save(any());
        verify(ack).acknowledge();
    }
}
