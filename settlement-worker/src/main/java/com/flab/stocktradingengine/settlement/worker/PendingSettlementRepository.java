package com.flab.stocktradingengine.settlement.worker;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * 정산 대기 미수금 저장소.
 * <p>settlementRef(PK) 존재 여부로 이미 저장한 정산 요청인지 판별해 중복 저장을 막는다.</p>
 */
public interface PendingSettlementRepository extends JpaRepository<PendingSettlement, Long> {

    /** T+2 스캔(a2-3) — 만기가 지난 status의 건을 찾는다. (status, due_at_epoch_millis) 복합 인덱스 사용. */
    List<PendingSettlement> findByStatusAndDueAtEpochMillisLessThanEqual(SettlementStatus status, long dueAtEpochMillis);
}
