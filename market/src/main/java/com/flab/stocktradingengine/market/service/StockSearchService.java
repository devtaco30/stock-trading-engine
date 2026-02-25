package com.flab.stocktradingengine.market.service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;

import org.springframework.data.domain.Pageable;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.dto.common.PagedResponse;
import com.flab.stocktradingengine.market.entity.Stock;
import com.flab.stocktradingengine.market.mapper.StockSearchViewMapper;
import com.flab.stocktradingengine.market.repository.QuoteRepository;
import com.flab.stocktradingengine.market.repository.StockRepository;
import com.flab.stocktradingengine.market.view.StockSearchView;

import lombok.RequiredArgsConstructor;

/**
 * 종목 검색 서비스 (market 도메인)
 */
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class StockSearchService {

    private final StockRepository stockRepository;
    private final QuoteRepository quoteRepository;

    /**
     * 키워드로 종목 검색 (종목명·종목코드 포함 검색, 페이지네이션)
     */
    public PagedResponse<StockSearchView> searchStocks(@NonNull String keyword, Pageable pageable) {
        List<Stock> stocks = stockRepository.findByStockNameContainingOrStockCode(keyword, keyword, pageable);
        long totalElements = stockRepository.countByStockNameContainingOrStockCode(keyword, keyword);
        List<StockSearchView> content = stocks.stream()
            .map(stock -> {
                String stockCode = Objects.requireNonNull(stock.getStockCode());
                BigDecimal currentPrice = quoteRepository.findById(stockCode)
                    .map(quote -> quote.getCurrentPrice())
                    .orElse(BigDecimal.ZERO);
                return StockSearchViewMapper.toView(stock, currentPrice);
            })
            .toList();
        return PagedResponse.of(content, pageable.getPageNumber(), pageable.getPageSize(), totalElements);
    }
}
