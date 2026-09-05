package com.flab.stocktradingengine.kafka.consumer;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;

import com.flab.stocktradingengine.account.exception.InsufficientResourceException;
import com.flab.stocktradingengine.kafka.event.OrderCancelRequestEvent;
import com.flab.stocktradingengine.kafka.event.OrderCancelledEvent;
import com.flab.stocktradingengine.kafka.event.OrderPlacedEvent;
import com.flab.stocktradingengine.kafka.event.OrderRequestEvent;
import com.flab.stocktradingengine.trading.entity.Order;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.service.OrderCommandService;
import com.flab.stocktradingengine.trading.service.OrderQueryService;
import com.flab.stocktradingengine.trading.view.PlaceOrderResultView;

/**
 * OrderRequestConsumer ack 전략·발행 테스트.
 *
 * <p>비즈니스 룰 위반(잔고 부족 등, {@code BusinessException} 계열)은 재시도해도 결과가 같으므로
 * ack 후 폐기해야 한다. 인프라 오류는 ack 미호출로 Kafka 재전달을 유도한다.</p>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class OrderRequestConsumerTest {

    @Mock
    OrderCommandService orderCommandService;
    @Mock
    OrderQueryService orderQueryService;
    @Mock
    @SuppressWarnings("rawtypes")
    KafkaTemplate kafkaTemplate;
    @Mock
    Acknowledgment ack;

    @InjectMocks
    OrderRequestConsumer consumer;

    private static final String STOCK = "005930";

    private ConsumerRecord<String, Object> record(Object value) {
        return new ConsumerRecord<>("order-requests", 0, 0L, "1001", value);
    }

    private OrderRequestEvent buyEvent() {
        return new OrderRequestEvent(1001L, STOCK, OrderSide.BUY, "LIMIT",
            new BigDecimal("70000"), 100, Instant.parse("2026-01-01T00:00:00Z"));
    }

    @Test
    @DisplayName("매수 요청 → placeBuyOrder 반영 후 orders 토픽에 OrderPlacedEvent 발행 + ack")
    void buy_publishesAndAcks() {
        when(orderCommandService.placeBuyOrder(any(), any()))
            .thenReturn(new PlaceOrderResultView(8801L, "PENDING", 0L, new BigDecimal("100")));

        consumer.consume(record(buyEvent()), ack);

        verify(orderCommandService).placeBuyOrder(any(), any());
        verify(kafkaTemplate).send(eq("orders." + STOCK), eq(STOCK), any(OrderPlacedEvent.class));
        verify(ack, times(1)).acknowledge();
    }

    @Test
    @DisplayName("비즈니스 예외(잔고 부족) → ack 후 폐기 · 발행 안 함 (재전달 방지)")
    void businessException_ackAndDiscard() {
        when(orderCommandService.placeBuyOrder(any(), any()))
            .thenThrow(new InsufficientResourceException("매수 가능 금액 초과"));

        consumer.consume(record(buyEvent()), ack);

        verify(ack, times(1)).acknowledge();
        verify(kafkaTemplate, never()).send(any(), any(), any());
    }

    @Test
    @DisplayName("인프라 예외 → 전파 + ack 미호출 (재전달 유도)")
    void infraException_propagateNoAck() {
        when(orderCommandService.placeBuyOrder(any(), any()))
            .thenThrow(new RuntimeException("DB 일시 장애"));

        assertThatThrownBy(() -> consumer.consume(record(buyEvent()), ack))
            .isInstanceOf(RuntimeException.class);

        verify(ack, never()).acknowledge();
    }

    @Test
    @DisplayName("취소 요청 → cancelOrder 후 OrderCancelledEvent 발행 + ack")
    void cancel_publishesAndAcks() {
        Order order = org.mockito.Mockito.mock(Order.class);
        when(orderQueryService.getOrder(9001L)).thenReturn(order);

        consumer.consume(record(new OrderCancelRequestEvent(1001L, 9001L, STOCK)), ack);

        verify(orderCommandService).cancelOrder(order);
        verify(kafkaTemplate).send(eq("orders." + STOCK), eq(STOCK), any(OrderCancelledEvent.class));
        verify(ack, times(1)).acknowledge();
    }
}
