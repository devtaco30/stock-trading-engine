package com.flab.stocktradingengine.account.projection.worker.messaging;

import java.util.List;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.flab.stocktradingengine.account.projection.worker.service.AccountProjectionUpserter;
import com.flab.stocktradingengine.kafka.event.AccountStateEvent;

import lombok.RequiredArgsConstructor;

/**
 * {@code account-state} 토픽을 배치로 소비해 {@link AccountProjectionUpserter}로 read model
 * 반영을 위임한다(계좌 상태 영속/프로젝션 트랙 U3-ii — 배치 안 같은 계좌 이벤트는 upserter가
 * 최신 seq 하나로 합친다). {@code spring.kafka.listener.type: batch}(application.yml)로 이
 * 리스너가 한 poll 배치를 통째로 받는다. ack는 배치 반영(=커밋)이 끝난 뒤 배치 전체에 한 번
 * 보낸다(커밋 전에 ack가 나가면 커밋 실패 시 오프셋만 넘어가 상태 갱신이 유실된다).
 */
@Component
@RequiredArgsConstructor
public class AccountStateConsumer {

    private final AccountProjectionUpserter accountProjectionUpserter;

    @KafkaListener(topics = "account-state", groupId = "account-projection-worker")
    public void consume(List<AccountStateEvent> events, Acknowledgment ack) {
        accountProjectionUpserter.upsertBatch(events);
        ack.acknowledge();
    }
}
