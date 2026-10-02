package com.flab.stocktradingengine.kafka.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.JacksonUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * 구간 분해 측정으로 v1 Kafka 이벤트에 시각 필드를 더해도 JSON이 호환되는지 고정한다. 필드가 없는 옛
 * 메시지는 시각 0으로, 모르는 필드가 섞인 메시지는 무시하고 읽혀야 한다(Spring Kafka JsonDeserializer가 쓰는
 * 것과 같은 ObjectMapper).
 */
class V1EventCompatibilityTest {

    private final ObjectMapper mapper = JacksonUtils.enhancedObjectMapper();

    @Test
    void 시각_필드가_없는_옛_OrderPlacedEvent_JSON은_시각_0으로_읽힌다() throws Exception {
        String oldJson = "{\"orderId\":1,\"accountId\":2,\"stockCode\":\"005930\",\"side\":\"BUY\","
            + "\"price\":70000,\"quantity\":3,\"orderAt\":\"2026-01-01T00:00:00Z\"}";

        OrderPlacedEvent event = mapper.readValue(oldJson, OrderPlacedEvent.class);

        assertThat(event.orderId()).isEqualTo(1L);
        assertThat(event.quantity()).isEqualTo(3);
        assertThat(event.receivedAtNanos()).isZero();
        assertThat(event.acceptedAtNanos()).isZero();
    }

    @Test
    void 새_OrderPlacedEvent는_JSON_왕복에서_시각이_보존된다() throws Exception {
        OrderPlacedEvent original = new OrderPlacedEvent(1L, 2L, "005930", OrderSide.SELL,
            new BigDecimal("70000"), 3, Instant.parse("2026-01-01T00:00:00Z"), 111L, 222L);

        OrderPlacedEvent copy = mapper.readValue(mapper.writeValueAsString(original), OrderPlacedEvent.class);

        assertThat(copy).isEqualTo(original);
    }

    @Test
    void 모르는_필드가_섞인_OrderPlacedEvent_JSON도_읽힌다() throws Exception {
        String json = "{\"orderId\":1,\"accountId\":2,\"stockCode\":\"005930\",\"side\":\"BUY\","
            + "\"price\":70000,\"quantity\":3,\"orderAt\":\"2026-01-01T00:00:00Z\",\"futureField\":9}";

        assertThat(mapper.readValue(json, OrderPlacedEvent.class).orderId()).isEqualTo(1L);
    }

    @Test
    void 시각_필드가_없는_옛_TradeFilledEvent_JSON은_시각_0으로_읽힌다() throws Exception {
        String oldJson = "{\"tradeId\":9,\"stockCode\":\"005930\",\"buyOrderId\":1,\"buyAccountId\":2,"
            + "\"sellOrderId\":3,\"sellAccountId\":4,\"filledQuantity\":5,\"matchPrice\":70000}";

        TradeFilledEvent event = mapper.readValue(oldJson, TradeFilledEvent.class);

        assertThat(event.tradeId()).isEqualTo(9L);
        assertThat(event.emittedAtNanos()).isZero();
        assertThat(event.aggressorOriginNanos()).isZero();
    }

    @Test
    void 새_TradeFilledEvent는_JSON_왕복에서_시각이_보존된다() throws Exception {
        TradeFilledEvent original = new TradeFilledEvent(9L, "005930", 1L, 2L, 3L, 4L, 5,
            new BigDecimal("70000"), 777L, 555L);

        TradeFilledEvent copy = mapper.readValue(mapper.writeValueAsString(original), TradeFilledEvent.class);

        assertThat(copy).isEqualTo(original);
    }

    @Test
    void 기존_인자_수_생성자는_시각을_0으로_채운다() {
        OrderPlacedEvent placed = new OrderPlacedEvent(1L, 2L, "005930", OrderSide.BUY,
            new BigDecimal("1"), 1, Instant.EPOCH);
        TradeFilledEvent filled = new TradeFilledEvent(9L, "005930", 1L, 2L, 3L, 4L, 5, new BigDecimal("1"));

        assertThat(placed.receivedAtNanos()).isZero();
        assertThat(filled.emittedAtNanos()).isZero();
    }
}
