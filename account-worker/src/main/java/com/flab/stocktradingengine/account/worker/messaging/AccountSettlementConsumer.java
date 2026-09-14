package com.flab.stocktradingengine.account.worker.messaging;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.kafka.event.SettlementResultEvent;

import lombok.RequiredArgsConstructor;

/**
 * {@code account-settlements} 토픽을 소비해 {@link AccountEngine}에 정산(T+2) 되돌림을 반영한다.
 * v2 신규 경로 — settlement가 "실제 잔고를 차감하라"고 보내는 명령을 여기서 받는다(v1엔 없었다).
 *
 * <p>A안(durable-before-ack): publish는 링버퍼에 넣기만 하는 비동기 발행이라
 * 발행 직후엔 아직 저널에 durable하게 남지 않았다. 여기서 바로 ack하면 저널 기록 전에 Kafka
 * 오프셋이 넘어가 유실 창이 생기므로, {@link AccountEngine#blockUntilJournaled}로 저널 기록을
 * 확인한 뒤에 ack한다(유실보다 중복이 안전 — 재전송은 settlementRef 멱등이 흡수한다).</p>
 */
@Component
@RequiredArgsConstructor
public class AccountSettlementConsumer {

    private final AccountEngine accountEngine;

    @KafkaListener(topics = "account-settlements", groupId = "account-worker")
    public void consume(SettlementResultEvent event, Acknowledgment ack) {
        long sequence = accountEngine.publishSettlement(event.settlementRef(), event.accountId(), event.amount());
        accountEngine.blockUntilJournaled(sequence);
        ack.acknowledge();
    }
}
