package com.flab.stocktradingengine.settlement.worker;

import java.util.concurrent.TimeUnit;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.kafka.KafkaTopics;
import com.flab.stocktradingengine.kafka.event.SettlementResultEvent;

import lombok.RequiredArgsConstructor;

/**
 * 정산 대기 건 하나를 {@code account-settlements}로 발행하고 SETTLED로 마킹하는 트랜잭션 경계.
 *
 * <p>반드시 발행 성공 확인 → markSettled 순서를 지킨다. 발행이 실패하면 예외를 던져 트랜잭션이
 * 롤백되고 PENDING이 유지된다 — 순서가 반대면 "정산됐다고 표시했는데 실제로는 못 보낸" 유실이
 * 생긴다. 발행 뒤·커밋 전에 크래시가 나면 다음 스캔이 같은 건을 재발행할 수 있지만, 받는 쪽
 * ({@code AccountState.applySettlement})이 settlementRef로 멱등이라 중복 반영되지 않는다
 * (안전한 at-least-once).</p>
 *
 * <p>markSettled 뒤 별도 save() 호출이 없다 — findById로 이 트랜잭션 안에서 얻은 영속 엔티티라
 * 커밋 시 dirty checking으로 UPDATE가 나간다.</p>
 */
@Component
@RequiredArgsConstructor
public class PendingSettlementSettler {

    private static final String TOPIC = KafkaTopics.accountSettlements();
    private static final long SEND_TIMEOUT_SECONDS = 5;

    private final PendingSettlementRepository pendingSettlementRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Transactional
    public void settle(Long settlementRef) {
        PendingSettlement pendingSettlement = pendingSettlementRepository.findById(settlementRef)
            .orElseThrow(() -> new IllegalStateException("정산 대상을 찾을 수 없습니다: settlementRef=" + settlementRef));

        SettlementResultEvent event = new SettlementResultEvent(
            pendingSettlement.getSettlementRef(), pendingSettlement.getAccountId(), pendingSettlement.getAmount());
        send(event);

        pendingSettlement.markSettled();
    }

    private void send(SettlementResultEvent event) {
        try {
            kafkaTemplate.send(TOPIC, String.valueOf(event.accountId()), event)
                .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("account-settlements 발행 실패: settlementRef=" + event.settlementRef(), e);
        }
    }
}
