package com.flab.stocktradingengine.matching.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import com.flab.stocktradingengine.kafka.event.TradeFilledEvent;
import com.flab.stocktradingengine.support.SnowflakeIdGenerator;
import com.flab.stocktradingengine.trading.matching.FillResult;

/**
 * 체결 하나가 매수·매도 계좌 앞으로 같은 tradeId로 fan-out 되는지 검증한다(ADR-018).
 * account-worker 쪽 컨슈머가 tradeId 멱등으로 중복을 거르므로, 두 메시지의 tradeId가
 * 다르면 한쪽이 반영 안 되는 사고로 이어진다.
 */
class AccountFillPublisherTest {

    private static final String STOCK = "005930";
    private static final String TOPIC = "account-fills";

    @Test
    void 체결하나를_매수_매도_accountId_키로_같은_tradeId로_두번_발행한다() {
        KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
        SnowflakeIdGenerator snowflakeIdGenerator = mock(SnowflakeIdGenerator.class);
        when(snowflakeIdGenerator.nextId()).thenReturn(9001L);

        AccountFillPublisher publisher = new AccountFillPublisher(kafkaTemplate, snowflakeIdGenerator);
        FillResult fill = new FillResult(1001L, 1L, 2001L, 2L, 4, new BigDecimal("10000"));

        publisher.onFill(STOCK, fill);

        ArgumentCaptor<Object> events = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate).send(eq(TOPIC), eq("1"), events.capture());
        verify(kafkaTemplate).send(eq(TOPIC), eq("2"), events.capture());

        TradeFilledEvent expected = new TradeFilledEvent(9001L, STOCK, 1001L, 1L, 2001L, 2L, 4, new BigDecimal("10000"));
        assertThat(events.getAllValues()).containsExactly(expected, expected);
    }
}
