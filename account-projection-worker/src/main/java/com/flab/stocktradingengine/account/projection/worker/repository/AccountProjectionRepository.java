package com.flab.stocktradingengine.account.projection.worker.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.flab.stocktradingengine.account.projection.worker.entity.AccountProjection;

/** 계좌 잔고 read model 저장소. accountId(PK)로 stale-guard upsert 대상을 조회한다. */
public interface AccountProjectionRepository extends JpaRepository<AccountProjection, Long> {
}
