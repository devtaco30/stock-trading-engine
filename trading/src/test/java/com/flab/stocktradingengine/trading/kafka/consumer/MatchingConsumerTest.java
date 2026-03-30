package com.flab.stocktradingengine.trading.kafka.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.kafka.core.KafkaTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;
import org.springframework.kafka.support.Acknowledgment;

import com.flab.stocktradingengine.trading.kafka.event.OrderCancelledEvent;
import com.flab.stocktradingengine.trading.kafka.event.OrderPlacedEvent;
import com.flab.stocktradingengine.trading.kafka.event.TradeFilledEvent;
import com.flab.stocktradingengine.trading.entity.Order;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.matching.OrderBook;
import com.flab.stocktradingengine.trading.matching.OrderBookRegistry;
import com.flab.stocktradingengine.trading.matching.OrderEntry;
import com.flab.stocktradingengine.trading.service.OrderQueryService;

@ExtendWith(MockitoExtension.class)
@DisplayName("MatchingConsumer 단위 테스트")
class MatchingConsumerTest {

    @Mock OrderBookRegistry orderBookRegistry;
    @Mock @SuppressWarnings("rawtypes") KafkaTemplate kafkaTemplate;
    @Mock OrderQueryService orderQueryService;
    @Mock StringRedisTemplate stringRedisTemplate;
    @Mock @SuppressWarnings("unchecked") ValueOperations<String, String> valueOps;
    @Mock ObjectMapper objectMapper;
    @Mock Acknowledgment ack;

    @InjectMocks
    MatchingConsumer consumer;

    private static final String STOCK_CODE = "005930";
    private static final String TOPIC = "orders." + STOCK_CODE;

    private OrderBook book;

    @BeforeEach
    void setUp() throws Exception {
        book = new OrderBook();
        lenient().when(orderBookRegistry.getOrCreate(STOCK_CODE)).thenReturn(book);
        lenient().when(stringRedisTemplate.opsForValue()).thenReturn(valueOps);
        lenient().when(objectMapper.writeValueAsString(any())).thenReturn("{}");
    }

    private ConsumerRecord<String, Object> record(Object value) {
        return new ConsumerRecord<>(TOPIC, 0, 0L, STOCK_CODE, value);
    }

    private OrderPlacedEvent buyEvent(long orderId, String price) {
        return new OrderPlacedEvent(orderId, 100L, STOCK_CODE, OrderSide.BUY,
            new BigDecimal(price), 10, Instant.now());
    }

    private OrderPlacedEvent sellEvent(long orderId, String price) {
        return new OrderPlacedEvent(orderId, 200L, STOCK_CODE, OrderSide.SELL,
            new BigDecimal(price), 10, Instant.now());
    }

    // ── 파티션 할당/반환 ──────────────────────────────────────────────────────

    @Nested
    @DisplayName("파티션 할당/반환")
    class PartitionLifecycle {

        @Test
        @DisplayName("파티션 할당 시 해당 종목 PENDING 주문을 OrderBook 에 로드")
        void 파티션_할당_시_PENDING_주문_로드() {
            Order mockOrder = pendingOrder(1L, OrderSide.BUY, "70000");
            when(orderQueryService.getPendingByStockCodeSortedByTime(STOCK_CODE))
                .thenReturn(List.of(mockOrder));

            consumer.onPartitionsAssigned(
                Map.of(new TopicPartition(TOPIC, 0), 0L),
                mock(ConsumerSeekCallback.class)
            );

            assertThat(book.containsOrder(1L)).isTrue();
        }

        @Test
        @DisplayName("파티션 할당 시 PENDING 없으면 OrderBook 비어있음")
        void 파티션_할당_PENDING_없으면_빈_OrderBook() {
            when(orderQueryService.getPendingByStockCodeSortedByTime(STOCK_CODE))
                .thenReturn(List.of());

            consumer.onPartitionsAssigned(
                Map.of(new TopicPartition(TOPIC, 0), 0L),
                mock(ConsumerSeekCallback.class)
            );

            assertThat(book.match()).isEmpty();
        }

        @Test
        @DisplayName("파티션 할당 시 이미 OrderBook 에 있는 주문은 중복 등록 안 함")
        void 파티션_할당_중복_주문_스킵() {
            book.addOrder(new OrderEntry(1L, 100L, STOCK_CODE, OrderSide.BUY,
                new BigDecimal("70000"), 10, Instant.now()));

            // containsOrder 체크 후 스킵 → getOrderId()만 필요
            Order mockOrder = mock(Order.class);
            when(mockOrder.getOrderId()).thenReturn(1L);
            when(orderQueryService.getPendingByStockCodeSortedByTime(STOCK_CODE))
                .thenReturn(List.of(mockOrder));

            consumer.onPartitionsAssigned(
                Map.of(new TopicPartition(TOPIC, 0), 0L),
                mock(ConsumerSeekCallback.class)
            );

            assertThat(book.containsOrder(1L)).isTrue();
        }

        @Test
        @DisplayName("파티션 반환 시 해당 종목 OrderBook 제거")
        void 파티션_반환_시_OrderBook_제거() {
            Collection<TopicPartition> partitions = List.of(new TopicPartition(TOPIC, 0));

            consumer.onPartitionsRevoked(partitions);

            verify(orderBookRegistry).removeBook(STOCK_CODE);
        }

        @Test
        @DisplayName("토픽명에 . 없는 파티션은 무시")
        void 알_수_없는_토픽_파티션_무시() {
            consumer.onPartitionsAssigned(
                Map.of(new TopicPartition("unknown", 0), 0L),
                mock(ConsumerSeekCallback.class)
            );

            verify(orderBookRegistry, never()).getOrCreate(any());
        }
    }

    // ── 라우팅 ───────────────────────────────────────────────────────────────

    @Test
    @DisplayName("OrderPlacedEvent 수신 시 OrderBook 에 주문 등록")
    void OrderPlacedEvent_수신_주문_등록() {
        consumer.consume(record(buyEvent(1L, "70000")), ack);

        assertThat(book.containsOrder(1L)).isTrue();
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("OrderCancelledEvent 수신 시 OrderBook 에서 취소 처리")
    void OrderCancelledEvent_수신_취소_처리() {
        book.addOrder(new OrderEntry(1L, 100L, STOCK_CODE, OrderSide.BUY,
            new BigDecimal("70000"), 10, Instant.now()));
        when(orderBookRegistry.get(STOCK_CODE)).thenReturn(book);

        consumer.consume(record(new OrderCancelledEvent(1L, STOCK_CODE)), ack);

        assertThat(book.containsOrder(1L)).isFalse();
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("알 수 없는 이벤트 타입 수신 시 OrderBook 접근 없이 ack 호출")
    void 알수없는_이벤트_타입_무시() {
        consumer.consume(record("unknown-event"), ack);

        verify(orderBookRegistry, never()).getOrCreate(any());
        verify(orderBookRegistry, never()).get(any());
        verify(ack).acknowledge();
    }

    // ── 체결 ─────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("체결 발생 시 TradeFilledEvent Kafka 발행 및 LTP 갱신")
    void 체결_발생_Kafka_발행_및_LTP_갱신() {
        book.addOrder(new OrderEntry(1L, 200L, STOCK_CODE, OrderSide.SELL,
            new BigDecimal("70000"), 10, Instant.now()));

        consumer.consume(record(buyEvent(2L, "70000")), ack);

        verify(kafkaTemplate).send(eq("fills." + STOCK_CODE), eq(STOCK_CODE), any(TradeFilledEvent.class));
        verify(orderBookRegistry).updateLastTradedPrice(eq(STOCK_CODE), eq(new BigDecimal("70000")));
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("체결 미발생 시 TradeFilledEvent 미발행")
    void 체결_미발생_Kafka_미발행() {
        consumer.consume(record(buyEvent(1L, "70000")), ack);

        verify(kafkaTemplate, never()).send(any(), any(), any());
        verify(ack).acknowledge();
    }

    // ── 멱등성 ───────────────────────────────────────────────────────────────

    @Test
    @DisplayName("동일 orderId 재제출 시 중복 등록 무시 — at-least-once 멱등성")
    void 동일_orderId_재제출_무시() {
        consumer.consume(record(buyEvent(1L, "70000")), ack);
        consumer.consume(record(buyEvent(1L, "70000")), ack);

        assertThat(book.containsOrder(1L)).isTrue();
        book.addOrder(new OrderEntry(2L, 200L, STOCK_CODE, OrderSide.SELL,
            new BigDecimal("70000"), 10, Instant.now()));
        var result = book.match();
        assertThat(result).isPresent();
        assertThat(book.match()).isEmpty();
    }

    // ── ack 보장 ─────────────────────────────────────────────────────────────

    @Test
    @DisplayName("OrderBook 접근 중 예외 발생 시에도 ack 는 항상 호출 (finally 보장)")
    void 예외_발생_시_ack_항상_호출() {
        doThrow(new RuntimeException("OrderBook 오류")).when(orderBookRegistry).getOrCreate(any());

        assertThatThrownBy(() -> consumer.consume(record(buyEvent(1L, "70000")), ack))
            .isInstanceOf(RuntimeException.class);

        verify(ack).acknowledge();
    }

    // ── 헬퍼 ─────────────────────────────────────────────────────────────────

    private Order pendingOrder(long orderId, OrderSide side, String price) {
        Order order = mock(Order.class);
        when(order.getOrderId()).thenReturn(orderId);
        when(order.getAccountId()).thenReturn(100L);
        when(order.getStockCode()).thenReturn(STOCK_CODE);
        when(order.getSide()).thenReturn(side);
        when(order.getPrice()).thenReturn(new BigDecimal(price));
        when(order.getQuantity()).thenReturn(10);
        when(order.getOrderAt()).thenReturn(Instant.now());
        when(order.getFilledQuantity()).thenReturn(0);
        return order;
    }
}
