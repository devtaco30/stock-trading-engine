package com.flab.stocktradingengine.account.projection.worker.messaging;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.flab.stocktradingengine.account.projection.worker.service.AccountProjectionUpserter;
import com.flab.stocktradingengine.kafka.event.AccountStateEvent;

import lombok.RequiredArgsConstructor;

/**
 * {@code account-state} 토픽을 소비해 {@link AccountProjectionUpserter}로 read model 반영을
 * 위임한다(계좌 상태 영속/프로젝션 트랙 U3). {@code SettlementRequestConsumer}와 같은 결 — ack는
 * 반영(=커밋)이 끝난 뒤에 보낸다(커밋 전에 ack가 나가면 커밋 실패 시 오프셋만 넘어가 상태
 * 갱신이 유실된다).
 */
@Component
@RequiredArgsConstructor
public class AccountStateConsumer {

    private final AccountProjectionUpserter accountProjectionUpserter;

    @KafkaListener(topics = "account-state", groupId = "account-projection-worker")
    public void consume(AccountStateEvent event, Acknowledgment ack) {
        accountProjectionUpserter.upsert(event);
        ack.acknowledge();
    }
}
