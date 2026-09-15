package com.flab.stocktradingengine.account.projection.worker.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.flab.stocktradingengine.account.projection.worker.entity.AccountProjection;
import com.flab.stocktradingengine.account.projection.worker.entity.AccountProjectionHolding;
import com.flab.stocktradingengine.account.projection.worker.repository.AccountProjectionHoldingRepository;
import com.flab.stocktradingengine.account.projection.worker.repository.AccountProjectionRepository;
import com.flab.stocktradingengine.kafka.event.AccountStateEvent;

/**
 * full-state 이벤트를 read model에 반영하는 stale-guard upsert 트랜잭션 경계(계좌 상태
 * 영속/프로젝션 트랙 U3). 실제 DB 왕복은 {@code AccountProjectionWorkerIntegrationTest}(임베디드
 * Kafka+H2)가 보고, 여기서는 저장소를 mock해 분기 로직만 좁게 본다.
 */
class AccountProjectionUpserterTest {

    private static final String STOCK = "005930";

    private final AccountProjectionRepository accountProjectionRepository = mock(AccountProjectionRepository.class);
    private final AccountProjectionHoldingRepository holdingRepository = mock(AccountProjectionHoldingRepository.class);
    private final AccountProjectionUpserter upserter =
        new AccountProjectionUpserter(accountProjectionRepository, holdingRepository);

    @Test
    void 신규_계좌_이벤트는_그대로_저장하고_보유를_교체한다() {
        when(accountProjectionRepository.findById(1L)).thenReturn(Optional.empty());
        AccountStateEvent event = new AccountStateEvent(1L, new BigDecimal("900000"), Map.of(STOCK, 10), 1L, 123L);

        upserter.upsert(event);

        ArgumentCaptor<AccountProjection> captor = ArgumentCaptor.forClass(AccountProjection.class);
        verify(accountProjectionRepository).save(captor.capture());
        AccountProjection saved = captor.getValue();
        assertThat(saved.getAccountId()).isEqualTo(1L);
        assertThat(saved.getBalance()).isEqualByComparingTo("900000");
        assertThat(saved.getSeq()).isEqualTo(1L);
        assertThat(saved.getUpdatedAt()).isEqualTo(Instant.ofEpochMilli(123L));

        verify(holdingRepository).deleteByAccountId(1L);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<AccountProjectionHolding>> holdingsCaptor = ArgumentCaptor.forClass(List.class);
        verify(holdingRepository).saveAll(holdingsCaptor.capture());
        assertThat(holdingsCaptor.getValue()).hasSize(1);
        assertThat(holdingsCaptor.getValue().get(0).getStockCode()).isEqualTo(STOCK);
        assertThat(holdingsCaptor.getValue().get(0).getQuantity()).isEqualTo(10);
    }

    @Test
    void seq가_저장된_값보다_크면_반영하고_보유를_교체한다() {
        AccountProjection existing = new AccountProjection(1L, new BigDecimal("900000"), 3L, Instant.ofEpochMilli(100L));
        when(accountProjectionRepository.findById(1L)).thenReturn(Optional.of(existing));
        AccountStateEvent event = new AccountStateEvent(1L, new BigDecimal("850000"), Map.of(STOCK, 8), 4L, 200L);

        upserter.upsert(event);

        verify(accountProjectionRepository).save(existing);
        assertThat(existing.getBalance()).isEqualByComparingTo("850000");
        assertThat(existing.getSeq()).isEqualTo(4L);
        verify(holdingRepository).deleteByAccountId(1L);
        verify(holdingRepository).saveAll(any());
    }

    @Test
    void seq가_저장된_값_이하면_stale로_보고_아무것도_반영하지_않는다() {
        AccountProjection existing = new AccountProjection(1L, new BigDecimal("900000"), 5L, Instant.ofEpochMilli(100L));
        when(accountProjectionRepository.findById(1L)).thenReturn(Optional.of(existing));
        AccountStateEvent staleEvent = new AccountStateEvent(1L, new BigDecimal("1"), Map.of(), 5L, 200L);

        upserter.upsert(staleEvent);

        verify(accountProjectionRepository, never()).save(any());
        verify(holdingRepository, never()).deleteByAccountId(anyLong());
        verify(holdingRepository, never()).saveAll(any());
        assertThat(existing.getBalance()).isEqualByComparingTo("900000"); // 원본 그대로
    }

    @Test
    void 보유가_비면_기존_행을_지우기만_하고_새로_넣지_않는다() {
        when(accountProjectionRepository.findById(1L)).thenReturn(Optional.empty());
        AccountStateEvent event = new AccountStateEvent(1L, new BigDecimal("900000"), Map.of(), 1L, 123L);

        upserter.upsert(event);

        verify(holdingRepository).deleteByAccountId(1L);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<AccountProjectionHolding>> holdingsCaptor = ArgumentCaptor.forClass(List.class);
        verify(holdingRepository).saveAll(holdingsCaptor.capture());
        assertThat(holdingsCaptor.getValue()).isEmpty();
    }
}
