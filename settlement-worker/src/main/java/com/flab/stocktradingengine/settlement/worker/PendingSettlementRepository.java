package com.flab.stocktradingengine.settlement.worker;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * 정산 대기 미수금 저장소.
 * <p>settlementRef(PK) 존재 여부로 이미 저장한 정산 요청인지 판별해 중복 저장을 막는다.</p>
 */
public interface PendingSettlementRepository extends JpaRepository<PendingSettlement, Long> {
}
