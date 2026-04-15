package com.flab.stocktradingengine.matching.kafka.consumer;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flab.stocktradingengine.kafka.KafkaTopics;
import com.flab.stocktradingengine.kafka.event.OrderCancelledEvent;
import com.flab.stocktradingengine.kafka.event.OrderPlacedEvent;
import com.flab.stocktradingengine.kafka.event.TradeFilledEvent;
import com.flab.stocktradingengine.matching.redis.LtpRedisRepository;
import com.flab.stocktradingengine.matching.redis.OrderbookRedisRepository;
import com.flab.stocktradingengine.trading.entity.Order;
import com.flab.stocktradingengine.trading.matching.FillResult;
import com.flab.stocktradingengine.trading.matching.OrderBook;
import com.flab.stocktradingengine.trading.matching.OrderBookRegistry;
import com.flab.stocktradingengine.trading.matching.OrderEntry;
import com.flab.stocktradingengine.trading.service.OrderQueryService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 매칭 컨슈머. orders.{stockCode} 토픽을 구독해 OrderBook 에 직접 처리한다.
 *
 * <h3>처리 흐름</h3>
 * <pre>
 * Kafka orders.{stockCode}
 *     │
 *     ├─ OrderPlacedEvent    → OrderBook.addOrder() + runMatch()
 *     └─ OrderCancelledEvent → OrderBook.cancelOrder()
 * </pre>
 *
 * <h3>Single Writer 보장</h3>
 * <p>Kafka 파티션 키를 종목코드로 지정하므로, 동일 종목의 메시지는
 * 항상 동일한 컨슈머 스레드에서 순서대로 처리된다.</p>
 *
 * <h3>호가창 복원 (스케일 아웃 대응)</h3>
 * <p>{@link ConsumerSeekAware#onPartitionsAssigned} 에서 이 인스턴스에 할당된
 * 파티션(= 종목코드)의 PENDING 주문만 로드한다.</p>
 *
 * <h3>멱등성</h3>
 * <p>at-least-once 환경에서 같은 메시지가 중복 수신될 수 있다.
 * {@code OrderBook.containsOrder()} 로 중복 주문을 감지해 무시한다.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MatchingConsumer implements ConsumerSeekAware {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final OrderBookRegistry orderBookRegistry;
    private final OrderQueryService orderQueryService;
    private final LtpRedisRepository ltpRedisRepository;
    private final OrderbookRedisRepository orderbookRedisRepository;
    private final ObjectMapper objectMapper;

    // ── 파티션 할당/반환 ────────────────────────────────────────────────────

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        assignments.keySet().forEach(tp -> {
            String stockCode = extractStockCode(tp.topic());
            if (stockCode == null) return;
            loadAndMatch(stockCode);
            log.info("[파티션 할당] 종목={} OrderBook 복원 완료", stockCode);
        });
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        partitions.forEach(tp -> {
            String stockCode = extractStockCode(tp.topic());
            if (stockCode == null) return;
            orderBookRegistry.removeBook(stockCode);
            log.info("[파티션 반환] 종목={} OrderBook 제거", stockCode);
        });
    }

    // ── 메시지 처리 ─────────────────────────────────────────────────────────

    @KafkaListener(topicPattern = "orders\\..*", groupId = "matching-engine")
    public void consume(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        Object event = record.value();
        try {
            if (event instanceof OrderPlacedEvent placed) {
                handlePlaced(placed);
            } else if (event instanceof OrderCancelledEvent cancelled) {
                handleCancelled(cancelled);
            } else {
                log.warn("[매칭 컨슈머] 알 수 없는 이벤트 타입: topic={} type={}",
                    record.topic(), event == null ? "null" : event.getClass().getSimpleName());
                ack.acknowledge();
                return;
            }
            String stockCode = record.key();
            if (stockCode != null) {
                writeOrderbookSnapshot(stockCode);
            }
            ack.acknowledge();
        } catch (IllegalArgumentException | IllegalStateException e) {
            // 비즈니스 룰 위반 — 재시도해도 결과가 같으므로 폐기
            log.warn("[매칭 컨슈머] 이벤트 폐기 (비즈니스 룰 위반): {}", e.getMessage());
            ack.acknowledge();
        }
        // 그 외 RuntimeException(Redis 장애, Kafka 발행 실패 등)은 전파 → ack 미호출 → Kafka 재전달
    }

    private void handlePlaced(OrderPlacedEvent event) {
        OrderBook book = orderBookRegistry.getOrCreate(event.stockCode());
        OrderEntry entry = toEntry(event);

        if (book.containsOrder(entry.getOrderId())) {
            log.warn("[멱등성] 중복 주문 무시: 종목={} 주문={}", event.stockCode(), event.orderId());
            return;
        }

        book.addOrder(entry);
        runMatch(event.stockCode(), book);
    }

    private void handleCancelled(OrderCancelledEvent event) {
        OrderBook book = orderBookRegistry.get(event.stockCode());
        if (book == null) return;
        book.cancelOrder(event.orderId());
    }

    // ── 내부 유틸 ───────────────────────────────────────────────────────────

    private void loadAndMatch(String stockCode) {
        OrderBook book = orderBookRegistry.getOrCreate(stockCode);
        List<Order> pending = orderQueryService.getPendingByStockCodeSortedByTime(stockCode);
        pending.forEach(order -> {
            if (!book.containsOrder(order.getOrderId())) {
                book.addOrder(toEntry(order));
            }
        });
        runMatch(stockCode, book);
    }

    private void runMatch(String stockCode, OrderBook book) {
        while (true) {
            // 체결 가능한 쌍을 찾아 매칭 시도.
            Optional<FillResult> result = book.match();
            if (result.isEmpty()) break;

            FillResult fill = result.get();
            log.info("[체결] 종목={} 매수주문={} 매도주문={} 수량={} 가격={}",
                stockCode, fill.buyOrderId(), fill.sellOrderId(),
                fill.filledQuantity(), fill.matchPrice());

            // 최근 체결가 갱신
            orderBookRegistry.updateLastTradedPrice(stockCode, fill.matchPrice());

            // 최근 체결가 Redis 저장
            ltpRedisRepository.set(stockCode, fill.matchPrice());

            // 체결 이벤트 발행
            kafkaTemplate.send(KafkaTopics.fills(stockCode), stockCode,
                new TradeFilledEvent(
                    stockCode,
                    fill.buyOrderId(), fill.buyAccountId(),
                    fill.sellOrderId(), fill.sellAccountId(),
                    fill.filledQuantity(), fill.matchPrice()
                ));
        }
    }

    /**
     * 호가창 스냅샷 저장
     */
    private void writeOrderbookSnapshot(String stockCode) {
        OrderBook book = orderBookRegistry.get(stockCode);
        if (book == null) return;
        List<Level> bids = book.getBidLevels(10).stream()
            .map(e -> new Level(e.getKey(), e.getValue()))
            .toList();
        List<Level> asks = book.getAskLevels(10).stream()
            .map(e -> new Level(e.getKey(), e.getValue()))
            .toList();
        try {
            orderbookRedisRepository.set(stockCode, objectMapper.writeValueAsString(new Snapshot(bids, asks)));
        } catch (JsonProcessingException e) {
            log.warn("[호가창 직렬화 실패] stockCode={}", stockCode, e);
        }
    }

    private record Level(BigDecimal price, int quantity) {}
    
    private record Snapshot(List<Level> bids, List<Level> asks) {}

    private static String extractStockCode(String topic) {
        int idx = topic.indexOf('.');
        return idx >= 0 ? topic.substring(idx + 1) : null;
    }

    private static OrderEntry toEntry(OrderPlacedEvent e) {
        return new OrderEntry(
            e.orderId(),
            e.accountId(),
            e.stockCode(),
            e.side(),
            e.price(),
            e.quantity(),
            e.orderAt()
        );
    }

    private static OrderEntry toEntry(Order order) {
        OrderEntry entry = new OrderEntry(
            order.getOrderId(),
            order.getAccountId(),
            order.getStockCode(),
            order.getSide(),
            order.getPrice(),
            order.getQuantity(),
            order.getOrderAt()
        );
        entry.addFilled(order.getFilledQuantity());
        return entry;
    }
}
