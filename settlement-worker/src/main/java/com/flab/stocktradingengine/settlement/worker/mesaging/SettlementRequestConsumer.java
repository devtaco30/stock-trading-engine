package com.flab.stocktradingengine.settlement.worker.mesaging;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.flab.stocktradingengine.kafka.event.SettlementRequestEvent;
import com.flab.stocktradingengine.settlement.worker.service.PendingSettlementRecorder;

import lombok.RequiredArgsConstructor;

/**
 * {@code settlement-requests} 토픽을 소비해 {@link PendingSettlementRecorder}로 저장을 위임한다.
 *
 * <p>ack는 반드시 저장(=커밋)이 끝난 뒤에 보낸다 — 이 리스너 자체는 트랜잭션이 아니다.
 * 커밋 전에 ack가 나가면(트랜잭션과 ack가 같은 메서드에 있으면 ack가 커밋보다 먼저 실행된다)
 * 커밋 실패 시 오프셋만 넘어가 정산 요청이 유실된다(at-most-once로 새는 경로).</p>
 */
@Component
@RequiredArgsConstructor
public class SettlementRequestConsumer {

    private final PendingSettlementRecorder pendingSettlementRecorder;

    @KafkaListener(topics = "settlement-requests", groupId = "settlement-worker")
    public void consume(SettlementRequestEvent event, Acknowledgment ack) {
        pendingSettlementRecorder.record(event);
        ack.acknowledge();
    }
}
