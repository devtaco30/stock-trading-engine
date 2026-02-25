package com.flab.stocktradingengine.market.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import com.flab.stocktradingengine.market.entity.Stock;

/**
 * 종목 마스터 저장소
 */
public interface StockRepository extends JpaRepository<Stock, String> {

    Optional<Stock> findByStockCode(String stockCode);

    /** 종목명 포함 검색 OR 종목코드 정확 일치 */
    List<Stock> findByStockNameContainingOrStockCode(String stockName, String stockCode, Pageable pageable);

    long countByStockNameContainingOrStockCode(String stockName, String stockCode);
}
