package com.flab.stocktradingengine.account.projection.worker.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import com.flab.stocktradingengine.account.projection.worker.entity.AccountProjectionHolding;
import com.flab.stocktradingengine.account.projection.worker.entity.AccountProjectionHoldingId;

/** 계좌 보유 read model 저장소. {@code AccountStateEvent} full-state 반영 시 계좌 단위로 전체 교체한다. */
public interface AccountProjectionHoldingRepository extends JpaRepository<AccountProjectionHolding, AccountProjectionHoldingId> {

    List<AccountProjectionHolding> findByAccountId(Long accountId);

    void deleteByAccountId(Long accountId);
}
