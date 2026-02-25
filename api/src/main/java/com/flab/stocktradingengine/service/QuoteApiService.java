package com.flab.stocktradingengine.service;

import org.springframework.stereotype.Service;

import com.flab.stocktradingengine.dto.market.QuoteDto;
import com.flab.stocktradingengine.market.service.QuoteService;

import lombok.RequiredArgsConstructor;

/**
 * 시세 조회 API용 서비스 (market.QuoteService 래퍼, DTO 변환)
 */
@Service
@RequiredArgsConstructor
public class QuoteApiService {

    private final QuoteService quoteService;

    /**
     * 현재가 조회 (없으면 null)
     */
    public QuoteDto getQuote(String stockCode) {
        return quoteService.getQuote(stockCode)
            .map(QuoteDto::from)
            .orElse(null);
    }
}
