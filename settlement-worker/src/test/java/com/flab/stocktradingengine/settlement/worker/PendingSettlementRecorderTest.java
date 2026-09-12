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

import com.flab.stocktradingengine.kafka.event.SettlementRequestEvent;

/**
 * 정산 요청을 PENDING으로 저장하는 트랜잭션 경계. 같은 settlementRef 재도착은 저장하지 않는지
 * 검증한다. ack는 이 클래스가 리턴(=커밋)한 뒤에 나가야 하므로(SettlementRequestConsumer 참고),
 * 이 클래스는 ack를 모른다 — 저장(커밋)만 책임진다.
 */
class PendingSettlementRecorderTest {

    @Test
    void 새_정산요청이면_PENDING으로_저장한다() {
        PendingSettlementRepository repository = mock(PendingSettlementRepository.class);
        when(repository.existsById(9001L)).thenReturn(false);
        PendingSettlementRecorder recorder = new PendingSettlementRecorder(repository);

        SettlementRequestEvent event = new SettlementRequestEvent(9001L, 1L, new BigDecimal("60000"), 123L);
        recorder.record(event);

        ArgumentCaptor<PendingSettlement> saved = ArgumentCaptor.forClass(PendingSettlement.class);
        verify(repository).save(saved.capture());
        PendingSettlement pendingSettlement = saved.getValue();
        assertThat(pendingSettlement.getSettlementRef()).isEqualTo(9001L);
        assertThat(pendingSettlement.getAccountId()).isEqualTo(1L);
        assertThat(pendingSettlement.getAmount()).isEqualByComparingTo(new BigDecimal("60000"));
        assertThat(pendingSettlement.getDueAtEpochMillis()).isEqualTo(123L);
        assertThat(pendingSettlement.getStatus()).isEqualTo(SettlementStatus.PENDING);
    }

    @Test
    void 같은_settlementRef가_재도착하면_저장하지_않는다() {
        PendingSettlementRepository repository = mock(PendingSettlementRepository.class);
        when(repository.existsById(9001L)).thenReturn(true);
        PendingSettlementRecorder recorder = new PendingSettlementRecorder(repository);

        SettlementRequestEvent event = new SettlementRequestEvent(9001L, 1L, new BigDecimal("60000"), 123L);
        recorder.record(event);

        verify(repository, never()).save(any());
    }
}
