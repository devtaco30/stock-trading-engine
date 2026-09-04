package com.flab.stocktradingengine.settlement.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.flab.stocktradingengine.settlement.entity.ProcessedFill;

/**
 * 처리 완료 체결 저장소.
 * <p>{@code tradeId}(PK) 존재 여부로 이미 반영한 체결인지 판별해 중복 처리를 막는다.</p>
 */
public interface ProcessedFillRepository extends JpaRepository<ProcessedFill, Long> {
}
