package com.flab.stocktradingengine.controller.market;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.flab.stocktradingengine.dto.common.ApiResponse;
import com.flab.stocktradingengine.dto.common.PagedResponse;
import com.flab.stocktradingengine.dto.market.QuoteDto;
import com.flab.stocktradingengine.dto.market.StockSearchDto;
import com.flab.stocktradingengine.service.QuoteApiService;
import com.flab.stocktradingengine.service.StockSearchApiService;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/v1/stocks")
@RequiredArgsConstructor
public class MarketController {

    private final QuoteApiService quoteApiService;
    private final StockSearchApiService stockSearchApiService;

    @GetMapping("/{stockCode}/quote")
    public ApiResponse<QuoteDto> quote(@PathVariable String stockCode) {
        return ApiResponse.of(quoteApiService.getQuote(stockCode));
    }

    @GetMapping("/search")
    public ApiResponse<PagedResponse<StockSearchDto>> search(
            @RequestParam String keyword,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        return ApiResponse.of(stockSearchApiService.searchStocks(keyword, page, size));
    }
}
