package com.flab.stocktradingengine.market.service;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.market.entity.Quote;
import com.flab.stocktradingengine.market.entity.Stock;
import com.flab.stocktradingengine.market.repository.QuoteRepository;
import com.flab.stocktradingengine.market.repository.StockRepository;
import com.flab.stocktradingengine.market.view.QuoteView;
import com.flab.stocktradingengine.market.view.StockInfo;

import lombok.RequiredArgsConstructor;

/**
 * 시세 조회 서비스 (market 도메인)
 */
@Service
@RequiredArgsConstructor
public class QuoteService {

    private final QuoteRepository quoteRepository;
    private final StockRepository stockRepository;

    /**
     * 종목 코드로 현재가 조회
     * Quote 와 Stock 정보를 fetch join으로 한 번에 조회.
     */
    public Optional<QuoteView> getQuote(@NonNull String stockCode) {
        return quoteRepository.findByIdWithStock(stockCode)
            .map(quote -> new QuoteView(
                quote.getStockCode(),
                quote.getStock().getStockName(),
                quote.getCurrentPrice(),
                quote.getPreviousClose(),
                quote.getChangeRate(),
                quote.getOpen(),
                quote.getHigh(),
                quote.getLow(),
                quote.getVolume()
            ));
    }

    /**
     * 종목 코드별 종목명·현재가. 종목이 없으면 empty.
     * 시세만 없으면 currentPrice는 0으로 반환.
     */
    @Transactional(readOnly = true)
    public Optional<StockInfo> getStockInfo(@NonNull String stockCode) {
        Optional<Stock> stock = stockRepository.findById(stockCode);
        if (stock.isEmpty()) {
            return Optional.empty();
        }
        Optional<Quote> quote = quoteRepository.findById(stockCode);
        BigDecimal currentPrice = quote.map(Quote::getCurrentPrice).orElse(BigDecimal.ZERO);
        BigDecimal previousClose = quote.map(Quote::getPreviousClose).orElse(BigDecimal.ZERO);
        return Optional.of(new StockInfo(stock.get().getStockName(), currentPrice, previousClose));
    }

    /**
     * 종목 코드 목록별 StockInfo 일괄 조회. N+1 방지용.
     * <p>Stock·Quote 각각 findAllById 1회로 조회 후 조립.</p>
     */
    @Transactional(readOnly = true)
    public Map<String, StockInfo> getStockInfoBatch(List<String> stockCodes) {
        if (stockCodes.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> stocks = stockRepository.findAllById(stockCodes).stream()
            .collect(Collectors.toMap(Stock::getStockCode, Stock::getStockName));
        Map<String, Quote> quoteMap = quoteRepository.findAllById(stockCodes).stream()
            .collect(Collectors.toMap(Quote::getStockCode, q -> q));

        return stockCodes.stream()
            .distinct()
            .collect(Collectors.toMap(
                code -> code,
                code -> {
                    Quote quote = quoteMap.get(code);
                    BigDecimal currentPrice = quote != null ? quote.getCurrentPrice() : BigDecimal.ZERO;
                    BigDecimal previousClose = quote != null ? quote.getPreviousClose() : BigDecimal.ZERO;
                    return new StockInfo(stocks.getOrDefault(code, code), currentPrice, previousClose);
                }
            ));
    }
}
