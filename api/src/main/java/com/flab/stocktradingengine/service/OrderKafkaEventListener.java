package com.flab.stocktradingengine.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import com.flab.stocktradingengine.trading.kafka.KafkaTopics;
import com.flab.stocktradingengine.trading.kafka.event.OrderCancelledEvent;
import com.flab.stocktradingengine.trading.kafka.event.OrderPlacedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 주문 관련 Kafka 발행 전담 리스너.
 *
 * OrderApiService 는 ApplicationEventPublisher 로 이벤트만 발행하고,
 * 실제 Kafka 발행 방법(토픽 결정, KafkaTemplate 호출)은 이 클래스가 책임진다.
 *
 * @TransactionalEventListener(phase = AFTER_COMMIT) 으로 DB 커밋 완료 후에만 발행을 보장한다.
 * 커밋 전 발행 시 MatchingConsumer 가 DB 에서 주문을 조회하지 못할 수 있다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderKafkaEventListener {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handleOrderPlaced(OrderPlacedEvent event) {
        String topic = KafkaTopics.orders(event.stockCode());
        kafkaTemplate.send(topic, event.stockCode(), event);
        log.info("[Kafka 발행] topic={} orderId={}", topic, event.orderId());
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handleOrderCancelled(OrderCancelledEvent event) {
        String topic = KafkaTopics.orders(event.stockCode());
        kafkaTemplate.send(topic, event.stockCode(), event);
        log.info("[Kafka 발행] topic={} orderId={}", topic, event.orderId());
    }
}
