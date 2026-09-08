package com.flab.stocktradingengine.settlement.worker;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.kafka.event.SettlementRequestEvent;

import lombok.RequiredArgsConstructor;

/**
 * {@code settlement-requests} 토픽을 소비해 미수금을 PENDING으로 저장한다(a2-2).
 *
 * <p>at-least-once 재전달에 대비해 저장 전에 존재 여부부터 조회한다(check-then-act) — 이미
 * 있으면 저장을 건너뛴다. settlementRef가 PK라 동시 삽입 경쟁 상태에서도 DB 유니크 제약이
 * 최후 안전망이 된다.</p>
 */
@Component
@RequiredArgsConstructor
public class SettlementRequestConsumer {

    private final PendingSettlementRepository pendingSettlementRepository;

    @KafkaListener(topics = "settlement-requests", groupId = "settlement-worker")
    @Transactional
    public void consume(SettlementRequestEvent event, Acknowledgment ack) {
        if (!pendingSettlementRepository.existsById(event.settlementRef())) {
            PendingSettlement pendingSettlement = new PendingSettlement(
                event.settlementRef(), event.accountId(), event.amount(), event.dueAtEpochMillis());
            pendingSettlementRepository.save(pendingSettlement);
        }
        ack.acknowledge();
    }
}
