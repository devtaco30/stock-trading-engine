package com.flab.stocktradingengine.settlement.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.flab.stocktradingengine.settlement.service.OrderSettlementService;
import com.flab.stocktradingengine.trading.kafka.event.TradeFilledEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.context.annotation.Profile;

/**
 * 체결 이벤트 컨슈머. fills.{stockCode} 토픽을 구독해 DB 에 반영한다.
 *
 * <h3>처리 흐름</h3>
 * <pre>
 * Kafka fills.{stockCode}
 *     │
 *     └─ TradeFilledEvent → fillBuyOrderPartially() + fillSellOrderPartially()
 * </pre>
 *
 * <h3>장애 처리 한계 (Level 1)</h3>
 * <p>DB 반영 실패 시 에러 로그만 남긴다. 프로덕션에서는 DLQ 또는 재시도 정책이 필요하다.
 * 서버 재시작 시 PENDING 주문을 DB 에서 다시 로드해 OrderBook 을 복원한다.</p>
 */
@Profile("settlement")
@Slf4j
@Component
@RequiredArgsConstructor
public class SettlementConsumer {

    private final OrderSettlementService orderSettlementService;

    @KafkaListener(topicPattern = "fills\\..*", groupId = "settlement-worker")
    public void consume(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        Object event = record.value();
        try {
            if (event instanceof TradeFilledEvent fill) {
                handleFill(fill);
            } else {
                log.warn("[정산 컨슈머] 알 수 없는 이벤트 타입: topic={} type={}",
                    record.topic(), event == null ? "null" : event.getClass().getSimpleName());
            }
        } finally {
            ack.acknowledge();
        }
    }

    private void handleFill(TradeFilledEvent fill) {
        try {
            orderSettlementService.fillBuyOrderPartially(
                fill.buyOrderId(), fill.filledQuantity(), fill.matchPrice());

            orderSettlementService.fillSellOrderPartially(
                fill.sellOrderId(), fill.filledQuantity());

            log.info("[체결 DB 반영 완료] 종목={} 매수={} 매도={} 수량={} 가격={}",
                fill.stockCode(), fill.buyOrderId(), fill.sellOrderId(),
                fill.filledQuantity(), fill.matchPrice());

        } catch (Exception e) {
            log.error("[체결 DB 반영 실패] 종목={} fill={}", fill.stockCode(), fill, e);
        }
    }
}
