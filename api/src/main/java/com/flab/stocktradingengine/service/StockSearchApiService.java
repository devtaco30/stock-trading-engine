package com.flab.stocktradingengine.service;

import java.util.List;

import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.dto.common.PagedResponse;
import com.flab.stocktradingengine.dto.market.StockSearchDto;
import com.flab.stocktradingengine.market.service.StockSearchService;
import com.flab.stocktradingengine.market.view.StockSearchView;

import lombok.RequiredArgsConstructor;

/**
 * 종목 검색 API용 서비스 (market.StockSearchService 래퍼, DTO 변환)
 */
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class StockSearchApiService {

    private final StockSearchService stockSearchService;

    /**
     * 키워드로 종목 검색 (페이지네이션)
     */
    public PagedResponse<StockSearchDto> searchStocks(String keyword, int page, int size) {
        PagedResponse<StockSearchView> fromMarket = stockSearchService.searchStocks(keyword, PageRequest.of(page, size));
        List<StockSearchDto> dtos = fromMarket.getData().stream().map(StockSearchDto::from).toList();
        return PagedResponse.of(dtos, fromMarket.getPagination());
    }
}
