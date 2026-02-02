package com.flab.stocktradingengine.market.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import com.flab.stocktradingengine.market.entity.Quote;

/**
 * 시세 저장소
 */
public interface QuoteRepository extends JpaRepository<Quote, String> {

    Optional<Quote> findByStockCode(String stockCode);
}
