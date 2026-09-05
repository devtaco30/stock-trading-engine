package com.flab.stocktradingengine.trading.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import com.flab.stocktradingengine.trading.command.BuyOrderCommand;
import com.flab.stocktradingengine.trading.command.SellOrderCommand;
import com.flab.stocktradingengine.trading.entity.Order;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.entity.OrderStatus;
import com.flab.stocktradingengine.trading.entity.OrderType;
import com.flab.stocktradingengine.trading.view.PlaceOrderResultView;

@ExtendWith(MockitoExtension.class)
@DisplayName("OrderCommandService - 멱등 접수(requestId) 오케스트레이션")
class OrderCommandServiceTest {

    @Mock OrderWriter orderWriter;
    @Mock OrderIdempotencyReader idempotencyReader;

    @InjectMocks
    OrderCommandService orderCommandService;

    private static final String REQUEST_ID = "req-1";
    private static final String STOCK = "005930";

    private BuyOrderCommand buyCommand() {
        return new BuyOrderCommand(1001L, STOCK, "LIMIT",
            new BigDecimal("70000"), 100, Instant.parse("2026-01-01T00:00:00Z"), REQUEST_ID);
    }

    private SellOrderCommand sellCommand() {
        return new SellOrderCommand(1001L, STOCK, "LIMIT",
            new BigDecimal("70000"), 100, Instant.parse("2026-01-01T00:00:00Z"), REQUEST_ID);
    }

    private Order pendingBuyOrder() {
        return Order.builder()
            .stockCode(STOCK)
            .side(OrderSide.BUY)
            .orderType(OrderType.LIMIT)
            .price(new BigDecimal("70000"))
            .quantity(100)
            .status(OrderStatus.PENDING)
            .orderAt(Instant.parse("2026-01-01T00:00:00Z"))
            .reservedMargin(new BigDecimal("3500000"))
            .requestedAt(Instant.parse("2026-01-01T00:00:00Z"))
            .requestId(REQUEST_ID)
            .build();
    }

    @Nested
    @DisplayName("매수")
    class Buy {

        @Test
        @DisplayName("신규 요청이면 writer 로 저장한다")
        void placesNewOrder() {
            PlaceOrderResultView written = new PlaceOrderResultView(1L, "PENDING", 0L, new BigDecimal("3500000"));
            when(idempotencyReader.findByRequestId(REQUEST_ID)).thenReturn(Optional.empty());
            when(orderWriter.writeBuyOrder(any(), any())).thenReturn(written);

            PlaceOrderResultView result = orderCommandService.placeBuyOrder(buyCommand(), () -> BigDecimal.ZERO);

            assertThat(result).isEqualTo(written);
            verify(orderWriter).writeBuyOrder(any(), any());
        }

        @Test
        @DisplayName("이미 처리된 requestId 면 저장하지 않고 기존 주문을 반환한다 (check-then-act)")
        void returnsExistingOnDuplicate() {
            when(idempotencyReader.findByRequestId(REQUEST_ID)).thenReturn(Optional.of(pendingBuyOrder()));

            PlaceOrderResultView result = orderCommandService.placeBuyOrder(buyCommand(), () -> BigDecimal.ZERO);

            assertThat(result.status()).isEqualTo("PENDING");
            assertThat(result.reservedMargin()).isEqualByComparingTo("3500000");
            verify(orderWriter, never()).writeBuyOrder(any(), any());
        }

        @Test
        @DisplayName("동시 삽입 경쟁(UNIQUE 위반)이면 승자 주문을 새 조회로 반환한다")
        void recoversWinnerOnRace() {
            when(idempotencyReader.findByRequestId(REQUEST_ID))
                .thenReturn(Optional.empty())          // check-then-act: 처음엔 없음
                .thenReturn(Optional.of(pendingBuyOrder())); // 경쟁 후 복구 조회
            when(orderWriter.writeBuyOrder(any(), any()))
                .thenThrow(new DataIntegrityViolationException("uq_orders_request_id"));

            PlaceOrderResultView result = orderCommandService.placeBuyOrder(buyCommand(), () -> BigDecimal.ZERO);

            assertThat(result.status()).isEqualTo("PENDING");
            verify(idempotencyReader, org.mockito.Mockito.times(2)).findByRequestId(REQUEST_ID);
        }

        @Test
        @DisplayName("경쟁 후 복구 조회도 비면 IllegalStateException")
        void throwsWhenRecoveryEmpty() {
            when(idempotencyReader.findByRequestId(REQUEST_ID))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.empty());
            when(orderWriter.writeBuyOrder(any(), any()))
                .thenThrow(new DataIntegrityViolationException("uq_orders_request_id"));

            assertThatThrownBy(() -> orderCommandService.placeBuyOrder(buyCommand(), () -> BigDecimal.ZERO))
                .isInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    @DisplayName("매도")
    class Sell {

        @Test
        @DisplayName("이미 처리된 requestId 면 저장하지 않고 기존 주문을 반환한다")
        void returnsExistingOnDuplicate() {
            when(idempotencyReader.findByRequestId(REQUEST_ID)).thenReturn(Optional.of(pendingBuyOrder()));

            PlaceOrderResultView result = orderCommandService.placeSellOrder(sellCommand());

            assertThat(result.status()).isEqualTo("PENDING");
            verify(orderWriter, never()).writeSellOrder(any());
        }

        @Test
        @DisplayName("신규 요청이면 writer 로 저장한다")
        void placesNewOrder() {
            PlaceOrderResultView written = new PlaceOrderResultView(2L, "PENDING", 0L, null);
            when(idempotencyReader.findByRequestId(REQUEST_ID)).thenReturn(Optional.empty());
            when(orderWriter.writeSellOrder(any())).thenReturn(written);

            PlaceOrderResultView result = orderCommandService.placeSellOrder(sellCommand());

            assertThat(result).isEqualTo(written);
            verify(orderWriter).writeSellOrder(eq(sellCommand()));
        }
    }
}
