package com.flab.stocktradingengine.settlement.worker.service;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.kafka.event.SettlementRequestEvent;
import com.flab.stocktradingengine.settlement.worker.entity.PendingSettlement;
import com.flab.stocktradingengine.settlement.worker.mesaging.SettlementRequestConsumer;
import com.flab.stocktradingengine.settlement.worker.repository.PendingSettlementRepository;

import lombok.RequiredArgsConstructor;

/**
 * 정산 요청을 PENDING으로 저장하는 트랜잭션 경계.
 *
 * <p>at-least-once 재전달에 대비해 저장 전에 존재 여부부터 조회한다(check-then-act) — 이미
 * 있으면 저장을 건너뛴다. settlementRef가 PK라 동시 삽입 경쟁 상태에서도 DB 유니크 제약이
 * 최후 안전망이 된다.</p>
 *
 * <p>{@link SettlementRequestConsumer}가 이 메서드 호출(=커밋) 뒤에 ack를 보내도록, 커밋과
 * ack를 분리하려고 리스너에서 트랜잭션 경계를 떼어 여기로 옮겼다 — DB 커밋 전에 ack가 나가면
 * 커밋 실패 시 오프셋만 넘어가 정산 요청이 유실된다(재전달이 안 옴).</p>
 */
@Component
@RequiredArgsConstructor
public class PendingSettlementRecorder {

    private final PendingSettlementRepository pendingSettlementRepository;

    @Transactional
    public void record(SettlementRequestEvent event) {
        if (!pendingSettlementRepository.existsById(event.settlementRef())) {
            PendingSettlement pendingSettlement = new PendingSettlement(
                event.settlementRef(), event.accountId(), event.amount(), event.dueAtEpochMillis());
            pendingSettlementRepository.save(pendingSettlement);
        }
    }
}
