package com.flab.stocktradingengine.market.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.flab.stocktradingengine.market.entity.Quote;

/**
 * 시세 저장소
 */
public interface QuoteRepository extends JpaRepository<Quote, String> {
    /**
     * 종목 코드로 시세 조회 (종목 정보 포함)
     * <p>N+1 방지용. stock 정보는 fetch join으로 한 번에 조회.</p>
     */
    @Query("SELECT q FROM Quote q JOIN FETCH q.stock WHERE q.stockCode = :stockCode")
    Optional<Quote> findByIdWithStock(String stockCode);
}
