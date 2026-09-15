package com.flab.stocktradingengine.api.projection.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.flab.stocktradingengine.api.projection.entity.AccountProjection;

/** 계좌 잔고 read model 조회 전용 저장소(계좌 상태 프로젝션 트랙 U4). accountId(PK)로 단건 조회. */
public interface AccountProjectionRepository extends JpaRepository<AccountProjection, Long> {
}
