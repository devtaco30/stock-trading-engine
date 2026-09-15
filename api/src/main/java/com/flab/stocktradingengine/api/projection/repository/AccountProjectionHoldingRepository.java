package com.flab.stocktradingengine.api.projection.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import com.flab.stocktradingengine.api.projection.entity.AccountProjectionHolding;
import com.flab.stocktradingengine.api.projection.entity.AccountProjectionHoldingId;

/** 계좌 보유 read model 조회 전용 저장소(계좌 상태 프로젝션 트랙 U4). 계좌 단위로 보유 전체 조회. */
public interface AccountProjectionHoldingRepository
        extends JpaRepository<AccountProjectionHolding, AccountProjectionHoldingId> {

    List<AccountProjectionHolding> findByAccountId(Long accountId);
}
