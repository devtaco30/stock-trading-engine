package com.flab.stocktradingengine.settlement.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import com.flab.stocktradingengine.settlement.entity.Unpaid;
import com.flab.stocktradingengine.settlement.entity.UnpaidStatus;

/**
 * 미결제 저장소
 */
public interface UnpaidRepository extends JpaRepository<Unpaid, Long> {

    List<Unpaid> findByAccount_AccountId(Long accountId);

    List<Unpaid> findByAccount_AccountIdAndStatus(Long accountId, UnpaidStatus status);

    java.util.Optional<Unpaid> findByUnpaidId(String unpaidId);
}
