package com.flab.stocktradingengine.account.worker;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.flab.stocktradingengine.account.disruptor.AccountEngine;
import com.flab.stocktradingengine.kafka.event.SettlementResultEvent;

import lombok.RequiredArgsConstructor;

/**
 * {@code account-settlements} 토픽을 소비해 {@link AccountEngine}에 정산(T+2) 되돌림을 반영한다.
 * v2 신규 경로 — settlement가 "실제 잔고를 차감하라"고 보내는 명령을 여기서 받는다(v1엔 없었다).
 *
 * <p>{@link AccountFillConsumer}와 같은 결: publish는 링버퍼에 넣기만 하는 비동기 발행이라
 * 여기서 예외가 나는 경우는 사실상 없다(실제 반영 성공·실패는 {@code AccountResultListener}
 * 콜백으로 나중에 갈린다). 그래서 발행 호출 뒤 바로 커밋한다.</p>
 */
@Component
@RequiredArgsConstructor
public class AccountSettlementConsumer {

    private final AccountEngine accountEngine;

    @KafkaListener(topics = "account-settlements", groupId = "account-worker")
    public void consume(SettlementResultEvent event, Acknowledgment ack) {
        accountEngine.publishSettlement(event.settlementRef(), event.accountId(), event.amount());
        ack.acknowledge();
    }
}
