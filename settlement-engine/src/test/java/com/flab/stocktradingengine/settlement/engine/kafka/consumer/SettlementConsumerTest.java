package com.flab.stocktradingengine.settlement.engine.kafka.consumer;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.math.BigDecimal;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import com.flab.stocktradingengine.exception.ResourceNotFoundException;
import com.flab.stocktradingengine.kafka.event.TradeFilledEvent;
import com.flab.stocktradingengine.settlement.service.OrderSettlementService;

/**
 * SettlementConsumer ack 전략 테스트.
 *
 * <p>at-least-once 환경에서 예외 종류에 따라 ack(오프셋 커밋)을 다르게 처리해야 한다.
 * 비즈니스 룰 위반은 재시도해도 결과가 같으므로 ack 후 폐기,
 * 인프라 오류는 ack 미호출로 Kafka 재전달을 유도한다.</p>
 */
@ExtendWith(MockitoExtension.class)
class SettlementConsumerTest {

    @Mock
    private OrderSettlementService orderSettlementService;

    @Mock
    private Acknowledgment ack;

    @InjectMocks
    private SettlementConsumer settlementConsumer;

    private static final String STOCK_CODE = "005930";

    private ConsumerRecord<String, Object> recordOf(Object value) {
        return new ConsumerRecord<>("fills." + STOCK_CODE, 0, 0L, STOCK_CODE, value);
    }

    private static final long TRADE_ID = 9001L;

    private TradeFilledEvent sampleFill() {
        return new TradeFilledEvent(TRADE_ID, STOCK_CODE, 8801L, 1001L, 8802L, 2002L, 100, new BigDecimal("70000"));
    }

    @Test
    @DisplayName("정상 체결 이벤트 → 서비스 반영 후 ack 1회")
    void normalFill_ack() {
        settlementConsumer.consume(recordOf(sampleFill()), ack);

        verify(orderSettlementService).fillTradePartially(eq(TRADE_ID), eq(8801L), eq(8802L), eq(100), any());
        verify(ack, times(1)).acknowledge();
    }

    @Test
    @DisplayName("비즈니스 룰 위반(IllegalArgumentException) → ack 후 폐기 (재전달 안 함)")
    void businessException_ackAndDiscard() {
        doThrow(new IllegalArgumentException("체결 수량 초과"))
            .when(orderSettlementService).fillTradePartially(anyLong(), anyLong(), anyLong(), anyInt(), any());

        settlementConsumer.consume(recordOf(sampleFill()), ack);

        verify(ack, times(1)).acknowledge();
    }

    @Test
    @DisplayName("비즈니스 예외(BusinessException: 주문/보유 없음) → ack 후 폐기")
    void businessBaseException_ackAndDiscard() {
        doThrow(new ResourceNotFoundException("Order not found"))
            .when(orderSettlementService).fillTradePartially(anyLong(), anyLong(), anyLong(), anyInt(), any());

        settlementConsumer.consume(recordOf(sampleFill()), ack);

        verify(ack, times(1)).acknowledge();
    }

    @Test
    @DisplayName("인프라 오류(RuntimeException) → 예외 전파 + ack 미호출 (재전달 유도)")
    void infraException_propagateNoAck() {
        doThrow(new RuntimeException("DB 일시 장애"))
            .when(orderSettlementService).fillTradePartially(anyLong(), anyLong(), anyLong(), anyInt(), any());

        assertThatThrownBy(() -> settlementConsumer.consume(recordOf(sampleFill()), ack))
            .isInstanceOf(RuntimeException.class);

        verify(ack, never()).acknowledge();
    }

    @Test
    @DisplayName("알 수 없는 이벤트 타입 → 서비스 미호출 + ack 1회 (폐기)")
    void unknownEvent_ackAndSkip() {
        settlementConsumer.consume(recordOf("알 수 없는 문자열 이벤트"), ack);

        verify(orderSettlementService, never())
            .fillTradePartially(anyLong(), anyLong(), anyLong(), anyInt(), any());
        verify(ack, times(1)).acknowledge();
    }
}
