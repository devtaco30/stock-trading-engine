package com.flab.stocktradingengine.account.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import com.flab.stocktradingengine.account.entity.AccountStatusHistory;

/**
 * 계좌 상태 변경 이력 저장소
 */
public interface AccountStatusHistoryRepository extends JpaRepository<AccountStatusHistory, Long> {

    List<AccountStatusHistory> findByAccount_AccountIdOrderByChangedAtDesc(Long accountId);
}
