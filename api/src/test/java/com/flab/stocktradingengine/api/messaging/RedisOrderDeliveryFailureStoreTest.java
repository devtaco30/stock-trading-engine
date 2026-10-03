package com.flab.stocktradingengine.api.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flab.stocktradingengine.trading.entity.OrderSide;

class RedisOrderDeliveryFailureStoreTest {

    @Test
    @SuppressWarnings("unchecked")
    void 키와_만료와_JSON_필드를_기록한다() throws Exception {
        StringRedisTemplate template = mock(StringRedisTemplate.class);
        ValueOperations<String, String> ops = mock(ValueOperations.class);
        when(template.opsForValue()).thenReturn(ops);
        RedisOrderDeliveryFailureStore store = new RedisOrderDeliveryFailureStore(template);
        PendingOrder order = new PendingOrder(OrderSide.SELL, 123L, "005930", new BigDecimal("70000"), 10, "req-1", 5L);

        store.put(order, "offer 결과=-2", 4, 1_700_000_000_000L);

        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(ops).set(org.mockito.ArgumentMatchers.eq("order-delivery-failed:req-1"), value.capture(),
            org.mockito.ArgumentMatchers.eq(Duration.ofHours(24)));
        JsonNode json = new ObjectMapper().readTree(value.getValue());
        assertThat(json.get("accountId").asLong()).isEqualTo(123L);
        assertThat(json.get("stockCode").asText()).isEqualTo("005930");
        assertThat(json.get("side").asText()).isEqualTo("SELL");
        assertThat(json.get("reason").asText()).isEqualTo("offer 결과=-2");
        assertThat(json.get("attempts").asInt()).isEqualTo(4);
        assertThat(json.get("failedAtEpochMillis").asLong()).isEqualTo(1_700_000_000_000L);
    }
}
