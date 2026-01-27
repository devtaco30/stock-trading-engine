package com.flab.stocktradingengine.controller.market;

import com.flab.stocktradingengine.dto.common.ApiResponse;
import com.flab.stocktradingengine.dto.common.PagedResponse;
import com.flab.stocktradingengine.dto.common.PaginationInfo;
import com.flab.stocktradingengine.dto.market.QuoteDto;
import com.flab.stocktradingengine.dto.market.StockSearchDto;
import com.flab.stocktradingengine.dummy.DummyMarketData;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/stocks")
public class MarketController {

    @GetMapping("/{stockCode}/quote")
    public ApiResponse<QuoteDto> quote(@PathVariable String stockCode) {
        return ApiResponse.of(DummyMarketData.getQuote(stockCode));
    }

    @GetMapping("/search")
    public ApiResponse<PagedResponse<StockSearchDto>> search(@RequestParam String keyword) {
        var results = DummyMarketData.searchStocks(keyword);
        PaginationInfo pagination = PaginationInfo.builder()
            .page(0)
            .size(10)
            .totalElements((long) results.size())
            .totalPages(1)
            .hasNext(false)
            .hasPrevious(false)
            .build();
        return ApiResponse.of(PagedResponse.of(results, pagination));
    }
}
