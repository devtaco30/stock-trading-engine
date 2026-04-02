package com.flab.stocktradingengine.kafka.consumer;

import java.math.BigDecimal;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.account.service.AccountService;
import com.flab.stocktradingengine.exception.ResourceNotFoundException;
import com.flab.stocktradingengine.kafka.KafkaTopics;
import com.flab.stocktradingengine.kafka.event.OrderCancelRequestEvent;
import com.flab.stocktradingengine.kafka.event.OrderCancelledEvent;
import com.flab.stocktradingengine.kafka.event.OrderPlacedEvent;
import com.flab.stocktradingengine.kafka.event.OrderRequestEvent;
import com.flab.stocktradingengine.trading.command.BuyOrderCommand;
import com.flab.stocktradingengine.trading.command.SellOrderCommand;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.service.OrderCommandService;
import com.flab.stocktradingengine.trading.view.PlaceOrderResultView;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 주문 접수 요청 컨슈머. order-requests 토픽을 소비해 잔고·보유 검증 후 주문을 DB 에 저장한다.
 *
 * <h3>accountId 파티셔닝 효과</h3>
 * <p>토픽 키 = accountId → 같은 계좌의 요청은 항상 같은 파티션 → 같은 컨슈머 스레드에서 직렬 처리.
 * DB 비관적 락 경합이 구조적으로 제거된다.</p>
 *
 * <h3>처리 흐름</h3>
 * <pre>
 * order-requests
 *   ├─ OrderRequestEvent       → OrderCommandService.placeBuy/SellOrder()
 *   │                              → orders.{stockCode} 에 OrderPlacedEvent 발행
 *   └─ OrderCancelRequestEvent → OrderCommandService.cancelOrder()
 *                                  → orders.{stockCode} 에 OrderCancelledEvent 발행
 * </pre>
 *
 * <h3>unpaidSum</h3>
 * <p>settlement 모듈 의존 금지(모듈 계층 제약)로 미결제 미수금은 0 으로 처리한다.
 * Phase 3 Saga 패턴 도입 시 개선 예정.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderRequestConsumer {

    private final OrderCommandService orderCommandService;
    private final AccountService accountService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = "order-requests", groupId = "order-engine")
    public void consume(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        Object event = record.value();
        try {
            if (event instanceof OrderRequestEvent orderRequest) {
                handleOrderRequest(orderRequest);
            } else if (event instanceof OrderCancelRequestEvent cancelRequest) {
                handleCancelRequest(cancelRequest);
            } else {
                log.warn("[주문 접수 컨슈머] 알 수 없는 이벤트 타입: type={}",
                    event == null ? "null" : event.getClass().getSimpleName());
            }
            ack.acknowledge();
        } catch (IllegalArgumentException | IllegalStateException e) {
            // 가격 제한폭 초과·잔고 부족 등 비즈니스 룰 위반 — 재시도해도 결과가 같으므로 폐기
            log.warn("[주문 접수 컨슈머] 주문 폐기 (비즈니스 룰 위반): {}", e.getMessage());
            ack.acknowledge();
        }
        // 그 외 RuntimeException(DB 장애 등)은 전파 → ack 미호출 → Kafka 재전달
    }

    private void handleOrderRequest(OrderRequestEvent event) {
        Account account = accountService.getAccount(event.accountId())
            .orElseThrow(() -> new ResourceNotFoundException("Account not found: " + event.accountId()));

        PlaceOrderResultView result;
        if (event.side() == OrderSide.BUY) {
            BuyOrderCommand command = new BuyOrderCommand(
                event.accountId(), event.stockCode(),
                event.orderType(), event.price(), event.quantity(), event.requestedAt());
            // unpaidSum: settlement 모듈 접근 불가(모듈 계층 제약) → 0 처리
            result = orderCommandService.placeBuyOrder(command, account, () -> BigDecimal.ZERO);
        } else {
            SellOrderCommand command = new SellOrderCommand(
                event.accountId(), event.stockCode(),
                event.orderType(), event.price(), event.quantity(), event.requestedAt());
            result = orderCommandService.placeSellOrder(command, account);
        }

        kafkaTemplate.send(
            KafkaTopics.orders(event.stockCode()),
            event.stockCode(),
            new OrderPlacedEvent(
                result.orderId(), event.accountId(),
                event.stockCode(), event.side(),
                event.price(), event.quantity(),
                event.requestedAt()
            )
        );
        log.info("[주문 접수 완료] 종목={} 주문={} side={}", event.stockCode(), result.orderId(), event.side());
    }

    private void handleCancelRequest(OrderCancelRequestEvent event) {
        orderCommandService.cancelOrder(event.orderId());
        kafkaTemplate.send(
            KafkaTopics.orders(event.stockCode()),
            event.stockCode(),
            new OrderCancelledEvent(event.orderId(), event.stockCode())
        );
        log.info("[취소 처리 완료] 종목={} 주문={}", event.stockCode(), event.orderId());
    }
}
