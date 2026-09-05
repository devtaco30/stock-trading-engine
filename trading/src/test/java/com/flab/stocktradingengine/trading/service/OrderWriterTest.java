package com.flab.stocktradingengine.trading.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.account.entity.Holding;
import com.flab.stocktradingengine.account.exception.InsufficientResourceException;
import com.flab.stocktradingengine.account.service.AccountService;
import com.flab.stocktradingengine.exception.InvalidRequestException;
import com.flab.stocktradingengine.trading.command.SellOrderCommand;
import com.flab.stocktradingengine.trading.entity.Order;
import com.flab.stocktradingengine.trading.repository.OrderRepository;
import com.flab.stocktradingengine.trading.view.PlaceOrderResultView;

@ExtendWith(MockitoExtension.class)
@DisplayName("OrderWriter - 매도 가용 수량 검증(over-sell 방지)")
class OrderWriterTest {

    @Mock OrderRepository orderRepository;
    @Mock AccountService accountService;

    @InjectMocks
    OrderWriter orderWriter;

    private static final Long ACCOUNT_ID = 1001L;
    private static final String STOCK = "005930";

    private final Account account = mock(Account.class);

    private SellOrderCommand sellCommand(int quantity) {
        return new SellOrderCommand(ACCOUNT_ID, STOCK, "LIMIT",
            new BigDecimal("70000"), quantity, Instant.parse("2026-01-01T00:00:00Z"), "req-sell");
    }

    private Holding holding(int quantity) {
        return new Holding(account, STOCK, quantity, new BigDecimal("70000"));
    }

    @Test
    @DisplayName("보유 100, PENDING 매도 0, 요청 100 → 접수")
    void placesWhenAvailable() {
        when(accountService.getHoldingByAccountIdForUpdate(ACCOUNT_ID, STOCK)).thenReturn(Optional.of(holding(100)));
        when(orderRepository.sumPendingSellQuantity(ACCOUNT_ID, STOCK)).thenReturn(0L);
        when(orderRepository.saveAndFlush(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

        PlaceOrderResultView result = orderWriter.writeSellOrder(sellCommand(100));

        assertThat(result.status()).isEqualTo("PENDING");
        verify(orderRepository).saveAndFlush(any(Order.class));
    }

    @Test
    @DisplayName("보유 100, PENDING 매도 잔량 80, 요청 30 → 110>100 이므로 거부(over-sell 차단)")
    void rejectsWhenPendingSellExceedsHolding() {
        when(accountService.getHoldingByAccountIdForUpdate(ACCOUNT_ID, STOCK)).thenReturn(Optional.of(holding(100)));
        when(orderRepository.sumPendingSellQuantity(ACCOUNT_ID, STOCK)).thenReturn(80L);

        assertThatThrownBy(() -> orderWriter.writeSellOrder(sellCommand(30)))
            .isInstanceOf(InsufficientResourceException.class)
            .hasMessageContaining("매도 가능 수량 초과");
        verify(orderRepository, never()).saveAndFlush(any(Order.class));
    }

    @Test
    @DisplayName("보유 종목이 없으면 거부")
    void rejectsWhenNoHolding() {
        when(accountService.getHoldingByAccountIdForUpdate(ACCOUNT_ID, STOCK)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> orderWriter.writeSellOrder(sellCommand(10)))
            .isInstanceOf(InvalidRequestException.class);
        verify(orderRepository, never()).saveAndFlush(any(Order.class));
    }

    @Test
    @DisplayName("보유 100, PENDING 매도 잔량 100, 요청 1 → 이미 전량 약정이라 거부")
    void rejectsWhenFullyCommitted() {
        when(accountService.getHoldingByAccountIdForUpdate(ACCOUNT_ID, STOCK)).thenReturn(Optional.of(holding(100)));
        when(orderRepository.sumPendingSellQuantity(ACCOUNT_ID, STOCK)).thenReturn(100L);

        assertThatThrownBy(() -> orderWriter.writeSellOrder(sellCommand(1)))
            .isInstanceOf(InsufficientResourceException.class);
        verify(orderRepository, never()).saveAndFlush(any(Order.class));
    }
}
