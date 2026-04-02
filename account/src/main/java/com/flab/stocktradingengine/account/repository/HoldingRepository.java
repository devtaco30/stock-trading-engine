package com.flab.stocktradingengine.account.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.flab.stocktradingengine.account.entity.Holding;

import jakarta.persistence.LockModeType;

/**
 * 보유 주식 저장소
 */
public interface HoldingRepository extends JpaRepository<Holding, Long> {

    Page<Holding> findByAccount_AccountId(Long accountId, Pageable pageable);

    Optional<Holding> findByAccount_AccountIdAndStockCode(Long accountId, String stockCode);

    List<Holding> findByAccount_AccountId(Long accountId);

    /** 매도 주문 접수 시 보유 수량 정합성용 락 (SELECT FOR UPDATE) — 내부 PK 기반 */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT h FROM Holding h WHERE h.account.id = :accountPk AND h.stockCode = :stockCode")
    Optional<Holding> findByAccount_IdAndStockCodeForUpdate(@Param("accountPk") Long accountPk, @Param("stockCode") String stockCode);

    /** 매도 주문 접수 시 보유 수량 정합성용 락 (SELECT FOR UPDATE) — Snowflake accountId 기반 */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT h FROM Holding h WHERE h.account.accountId = :accountId AND h.stockCode = :stockCode")
    Optional<Holding> findByAccount_AccountIdAndStockCodeForUpdate(@Param("accountId") Long accountId, @Param("stockCode") String stockCode);
}
