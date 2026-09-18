package com.flab.stocktradingengine.matching.kafka.consumer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.flab.stocktradingengine.exception.BusinessException;
import com.flab.stocktradingengine.kafka.KafkaTopics;
import com.flab.stocktradingengine.kafka.event.OrderCancelledEvent;
import com.flab.stocktradingengine.kafka.event.OrderPlacedEvent;
import com.flab.stocktradingengine.kafka.event.TradeFilledEvent;
import com.flab.stocktradingengine.matching.kafka.partition.StockPartitionResolver;
import com.flab.stocktradingengine.matching.redis.LtpRedisRepository;
import com.flab.stocktradingengine.matching.redis.OrderbookRedisRepository;
import com.flab.stocktradingengine.support.SnowflakeIdGenerator;
import com.flab.stocktradingengine.trading.entity.Order;
import com.flab.stocktradingengine.trading.matching.FillResult;
import com.flab.stocktradingengine.trading.matching.OrderBook;
import com.flab.stocktradingengine.trading.matching.OrderBookRegistry;
import com.flab.stocktradingengine.trading.matching.OrderEntry;
import com.flab.stocktradingengine.trading.service.OrderQueryService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 매칭 컨슈머. orders 토픽을 구독해 OrderBook 에 직접 처리한다.
 *
 * <h3>처리 흐름</h3>
 * <pre>
 * Kafka orders (파티션 N개, key=stockCode)
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
 * <p>토픽이 하나라 파티션 하나에 종목이 여럿 실린다. 그래서 파티션 번호에서 종목을
 * 거꾸로 알아낼 수 없다. {@link ConsumerSeekAware#onPartitionsAssigned} 에서는
 * PENDING 주문이 남은 종목을 DB 에서 받아 {@link StockPartitionResolver} 로
 * 파티션을 계산하고, 할당받은 파티션에 속하는 종목만 복원한다.</p>
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
    private final SnowflakeIdGenerator snowflakeIdGenerator;
    private final StockPartitionResolver stockPartitionResolver;

    // ── 파티션 할당/반환 ────────────────────────────────────────────────────

    /**
     * 할당받은 파티션에 실리는 종목의 호가창을 DB 의 PENDING 주문으로 복원한다.
     * 복원이 필요한 종목은 PENDING 주문이 남은 종목뿐이라 그 목록만 조회한다.
     */
    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        Set<Integer> assignedPartitions = partitionNumbersOf(assignments.keySet());
        List<String> pendingStockCodes = orderQueryService.getPendingStockCodes();
        List<String> restoreTargets = stockPartitionResolver.filterByPartitions(pendingStockCodes, assignedPartitions);

        restoreTargets.forEach(stockCode -> {
            loadAndMatch(stockCode);
            log.info("[파티션 할당] 종목={} OrderBook 복원 완료", stockCode);
        });
    }

    /**
     * 반납한 파티션에 실리는 종목의 호가창을 비운다. 그 파티션은 다른 인스턴스가 받으므로
     * 이 인스턴스의 인메모리 상태를 남겨두면 조회 결과가 실제와 어긋난다.
     * 반납은 리밸런스를 붙잡고 있는 구간이라 DB 를 조회하지 않고 올라와 있는 종목만 본다.
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        Set<Integer> revokedPartitions = partitionNumbersOf(partitions);
        Set<String> loadedStockCodes = orderBookRegistry.stockCodes();
        List<String> removeTargets = stockPartitionResolver.filterByPartitions(loadedStockCodes, revokedPartitions);

        removeTargets.forEach(stockCode -> {
            orderBookRegistry.removeBook(stockCode);
            log.info("[파티션 반환] 종목={} OrderBook 제거", stockCode);
        });
    }

    private static Set<Integer> partitionNumbersOf(Collection<TopicPartition> partitions) {
        return partitions.stream()
            .map(TopicPartition::partition)
            .collect(Collectors.toSet());
    }

    // ── 메시지 처리 ─────────────────────────────────────────────────────────

    @KafkaListener(topics = "orders", groupId = "matching-engine")
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
        } catch (BusinessException | IllegalArgumentException | IllegalStateException e) {
            // 비즈니스 룰 위반(BusinessException) 및 도메인 불변식 위반(IAE/ISE) — 재시도해도 결과가 같으므로 폐기
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

            // 체결 이벤트 발행 (tradeId = 체결 단위 멱등키, settlement 중복 반영 방지)
            long tradeId = snowflakeIdGenerator.nextId();
            kafkaTemplate.send(KafkaTopics.fills(), stockCode,
                new TradeFilledEvent(
                    tradeId,
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
        orderbookRedisRepository.saveSnapshot(stockCode, book.getBidLevels(10), book.getAskLevels(10));
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
