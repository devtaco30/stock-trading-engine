package com.flab.stocktradingengine.settlement.worker;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.kafka.support.Acknowledgment;

import com.flab.stocktradingengine.kafka.event.SettlementRequestEvent;
import com.flab.stocktradingengine.settlement.worker.mesaging.SettlementRequestConsumer;
import com.flab.stocktradingengine.settlement.worker.service.PendingSettlementRecorder;

/**
 * ack가 저장(=커밋)보다 먼저 나가지 않는지 검증한다. 커밋 전에 ack가 나가면 커밋 실패 시
 * 오프셋만 넘어가 정산 요청이 유실된다 — 그래서 record()가 ack.acknowledge()보다 먼저
 * 호출되는 순서 자체가 이 컨슈머의 핵심 계약이다.
 */
class SettlementRequestConsumerTest {

    @Test
    void 저장을_끝낸_뒤에_ack를_보낸다() {
        PendingSettlementRecorder recorder = mock(PendingSettlementRecorder.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        SettlementRequestConsumer consumer = new SettlementRequestConsumer(recorder);

        SettlementRequestEvent event = new SettlementRequestEvent(9001L, 1L, new BigDecimal("60000"), 123L);
        consumer.consume(event, ack);

        InOrder order = inOrder(recorder, ack);
        order.verify(recorder).record(event);
        order.verify(ack).acknowledge();
    }
}
