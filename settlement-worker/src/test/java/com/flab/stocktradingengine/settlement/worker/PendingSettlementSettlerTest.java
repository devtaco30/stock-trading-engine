package com.flab.stocktradingengine.settlement.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import com.flab.stocktradingengine.kafka.KafkaTopics;
import com.flab.stocktradingengine.kafka.event.SettlementResultEvent;
import com.flab.stocktradingengine.settlement.worker.entity.PendingSettlement;
import com.flab.stocktradingengine.settlement.worker.entity.SettlementStatus;
import com.flab.stocktradingengine.settlement.worker.repository.PendingSettlementRepository;
import com.flab.stocktradingengine.settlement.worker.service.PendingSettlementSettler;

/**
 * 발행 성공 확인 → markSettled 순서를 검증한다. 발행이 실패하면 markSettled가 호출되지 않아야
 * 한다 — 순서가 반대면 "정산됐다고 표시했는데 실제로는 못 보낸" 유실이 생긴다.
 */
class PendingSettlementSettlerTest {

    private static final String TOPIC = KafkaTopics.accountSettlements();

    @Test
    void 발행에_성공하면_이벤트를_보내고_SETTLED로_마킹한다() {
        PendingSettlementRepository repository = mock(PendingSettlementRepository.class);
        PendingSettlement pendingSettlement = new PendingSettlement(9001L, 1L, new BigDecimal("60000"), 123L);
        when(repository.findById(9001L)).thenReturn(Optional.of(pendingSettlement));
        KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send(eq(TOPIC), any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        PendingSettlementSettler settler = new PendingSettlementSettler(repository, kafkaTemplate);

        settler.settle(9001L);

        ArgumentCaptor<Object> sentEvent = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate).send(eq(TOPIC), eq("1"), sentEvent.capture());
        assertThat(sentEvent.getValue()).isEqualTo(new SettlementResultEvent(9001L, 1L, new BigDecimal("60000")));
        assertThat(pendingSettlement.getStatus()).isEqualTo(SettlementStatus.SETTLED);
    }

    @Test
    void 발행에_실패하면_예외를_던지고_SETTLED로_마킹하지_않는다() {
        PendingSettlementRepository repository = mock(PendingSettlementRepository.class);
        PendingSettlement pendingSettlement = new PendingSettlement(9001L, 1L, new BigDecimal("60000"), 123L);
        when(repository.findById(9001L)).thenReturn(Optional.of(pendingSettlement));
        KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
        CompletableFuture<SendResult<String, Object>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new ExecutionException("broker down", new RuntimeException()));
        when(kafkaTemplate.send(eq(TOPIC), any(), any())).thenReturn(failed);
        PendingSettlementSettler settler = new PendingSettlementSettler(repository, kafkaTemplate);

        assertThatThrownBy(() -> settler.settle(9001L)).isInstanceOf(IllegalStateException.class);

        assertThat(pendingSettlement.getStatus()).isEqualTo(SettlementStatus.PENDING);
    }
}
