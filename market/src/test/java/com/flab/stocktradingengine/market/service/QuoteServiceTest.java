package com.flab.stocktradingengine.market.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.flab.stocktradingengine.market.entity.Quote;
import com.flab.stocktradingengine.market.entity.Stock;
import com.flab.stocktradingengine.market.repository.QuoteRepository;
import com.flab.stocktradingengine.market.repository.StockRepository;
import com.flab.stocktradingengine.market.view.QuoteView;
import com.flab.stocktradingengine.market.view.StockInfo;

/**
 * market 모듈 {@link QuoteService} 단위 테스트.
 * <p>Repository를 mock하여 서비스 로직만 검증한다.</p>
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("QuoteService 단위 테스트")
class QuoteServiceTest {

    @Mock
    QuoteRepository quoteRepository;

    @Mock
    StockRepository stockRepository;

    @InjectMocks
    QuoteService quoteService;

    @Nested
    @DisplayName("getQuote")
    class GetQuote {

        @Test
        @DisplayName("시세·종목 있으면 QuoteView Optional 반환")
        void returnsQuoteViewWhenQuoteAndStockExist() {
            String stockCode = "005930";
            Quote quote = mockQuote(stockCode, new BigDecimal("70000"), new BigDecimal("69500"),
                new BigDecimal("0.72"), new BigDecimal("69600"), new BigDecimal("70100"),
                new BigDecimal("69400"), 1_000_000L);
            Stock stock = mockStock(stockCode, "삼성전자");

            when(quoteRepository.findByIdWithStock(stockCode)).thenReturn(Optional.of(quote));
            when(quote.getStock()).thenReturn(stock);

            Optional<QuoteView> result = quoteService.getQuote(stockCode);

            assertTrue(result.isPresent());
            QuoteView view = result.get();
            assertEquals(stockCode, view.stockCode());
            assertEquals("삼성전자", view.stockName());
            assertEquals(0, new BigDecimal("70000").compareTo(view.currentPrice()));
            assertEquals(0, new BigDecimal("69500").compareTo(view.previousClose()));
            assertEquals(1_000_000L, view.volume());
            verify(quoteRepository).findByIdWithStock(stockCode);
            verify(quote).getStock();
        }

        @Test
        @DisplayName("시세 없으면 empty 반환")
        void returnsEmptyWhenQuoteNotExists() {
            when(quoteRepository.findByIdWithStock("999999")).thenReturn(Optional.empty());

            Optional<QuoteView> result = quoteService.getQuote("999999");

            assertTrue(result.isEmpty());
            verify(quoteRepository).findByIdWithStock("999999");
        }
    }

    @Nested
    @DisplayName("getStockInfo")
    class GetStockInfo {

        @Test
        @DisplayName("종목·시세 있으면 StockInfo 반환")
        void returnsStockInfoWhenStockAndQuoteExist() {
            String stockCode = "005930";
            Stock stock = mockStock(stockCode, "삼성전자");
            Quote quote = mockQuote(stockCode, new BigDecimal("70000"), new BigDecimal("69000"), null, null, null, null, null);
            when(stockRepository.findById(stockCode)).thenReturn(Optional.of(stock));
            when(quoteRepository.findById(stockCode)).thenReturn(Optional.of(quote));

            Optional<StockInfo> result = quoteService.getStockInfo(stockCode);

            assertTrue(result.isPresent());
            assertEquals("삼성전자", result.get().stockName());
            assertEquals(0, new BigDecimal("70000").compareTo(result.get().currentPrice()));
            assertEquals(0, new BigDecimal("69000").compareTo(result.get().previousClose()));
            verify(stockRepository).findById(stockCode);
            verify(quoteRepository).findById(stockCode);
        }

        @Test
        @DisplayName("종목 없으면 empty")
        void returnsEmptyWhenStockNotExists() {
            when(stockRepository.findById("999999")).thenReturn(Optional.empty());

            Optional<StockInfo> result = quoteService.getStockInfo("999999");

            assertTrue(result.isEmpty());
            verify(stockRepository).findById("999999");
        }

        @Test
        @DisplayName("시세 없으면 currentPrice 0")
        void currentPriceZeroWhenQuoteNotExists() {
            String stockCode = "005930";
            Stock stock = mockStock(stockCode, "삼성전자");
            when(stockRepository.findById(stockCode)).thenReturn(Optional.of(stock));
            when(quoteRepository.findById(stockCode)).thenReturn(Optional.empty());

            Optional<StockInfo> result = quoteService.getStockInfo(stockCode);

            assertTrue(result.isPresent());
            assertEquals("삼성전자", result.get().stockName());
            assertEquals(0, BigDecimal.ZERO.compareTo(result.get().currentPrice()));
        }
    }

    @Nested
    @DisplayName("getStockInfoBatch")
    class GetStockInfoBatch {

        @Test
        @DisplayName("빈 목록이면 emptyMap 반환, repository 미호출")
        @SuppressWarnings("null") // List.of()는 null이 아님, @NonNull 파라미터에 정상 전달
        void returnsEmptyMapWhenListEmpty() {
            Map<String, StockInfo> result = quoteService.getStockInfoBatch(List.of());
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("여러 종목 조회 시 코드별 StockInfo 반환")
        @SuppressWarnings("null")
        void returnsMapByStockCode() {
            List<String> codes = List.of("005930", "000660");
            Stock stock1 = mockStock("005930", "삼성전자");
            Stock stock2 = mockStock("000660", "SK하이닉스");
            Quote quote1 = mockQuote("005930", new BigDecimal("70000"), null, null, null, null, null, null);
            Quote quote2 = mockQuote("000660", new BigDecimal("120000"), null, null, null, null, null, null);

            when(stockRepository.findAllById(codes)).thenReturn(List.of(stock1, stock2));
            when(quoteRepository.findAllById(codes)).thenReturn(List.of(quote1, quote2));

            Map<String, StockInfo> result = quoteService.getStockInfoBatch(codes);

            assertEquals(2, result.size());
            assertEquals("삼성전자", result.get("005930").stockName());
            assertEquals(0, new BigDecimal("70000").compareTo(result.get("005930").currentPrice()));
            assertEquals("SK하이닉스", result.get("000660").stockName());
            assertEquals(0, new BigDecimal("120000").compareTo(result.get("000660").currentPrice()));
            verify(stockRepository).findAllById(codes);
            verify(quoteRepository).findAllById(codes);
        }

        @Test
        @DisplayName("일부 종목만 시세 없으면 해당 종목 currentPrice 0")
        @SuppressWarnings("null")
        void missingQuoteUsesZeroPrice() {
            List<String> codes = List.of("005930", "000660");
            Stock stock1 = mockStock("005930", "삼성전자");
            Stock stock2 = mockStock("000660", "SK하이닉스");
            Quote quote1 = mockQuote("005930", new BigDecimal("70000"), null, null, null, null, null, null);
            when(stockRepository.findAllById(codes)).thenReturn(List.of(stock1, stock2));
            when(quoteRepository.findAllById(codes)).thenReturn(List.of(quote1));

            Map<String, StockInfo> result = quoteService.getStockInfoBatch(codes);

            assertEquals(2, result.size());
            assertEquals(0, new BigDecimal("70000").compareTo(result.get("005930").currentPrice()));
            assertEquals(0, BigDecimal.ZERO.compareTo(result.get("000660").currentPrice()));
        }

        @Test
        @DisplayName("중복 종목코드 있어도 distinct 기준으로 한 번씩만 매핑")
        @SuppressWarnings("null")
        void distinctStockCodesOnly() {
            List<String> codes = List.of("005930", "005930");
            Stock stock = mockStock("005930", "삼성전자");
            Quote quote = mockQuote("005930", new BigDecimal("70000"), null, null, null, null, null, null);
            when(stockRepository.findAllById(anyList())).thenReturn(List.of(stock));
            when(quoteRepository.findAllById(anyList())).thenReturn(List.of(quote));

            Map<String, StockInfo> result = quoteService.getStockInfoBatch(codes);

            assertEquals(1, result.size());
            assertTrue(result.containsKey("005930"));
        }
    }

    private static Quote mockQuote(String stockCode, BigDecimal currentPrice, BigDecimal previousClose,
                                  BigDecimal changeRate, BigDecimal open, BigDecimal high, BigDecimal low, Long volume) {
        Quote quote = org.mockito.Mockito.mock(Quote.class);
        lenient().when(quote.getStockCode()).thenReturn(stockCode);
        lenient().when(quote.getCurrentPrice()).thenReturn(currentPrice);
        lenient().when(quote.getPreviousClose()).thenReturn(previousClose);
        lenient().when(quote.getChangeRate()).thenReturn(changeRate);
        lenient().when(quote.getOpen()).thenReturn(open);
        lenient().when(quote.getHigh()).thenReturn(high);
        lenient().when(quote.getLow()).thenReturn(low);
        lenient().when(quote.getVolume()).thenReturn(volume);
        return quote;
    }

    private static Stock mockStock(String stockCode, String stockName) {
        Stock stock = org.mockito.Mockito.mock(Stock.class);
        lenient().when(stock.getStockCode()).thenReturn(stockCode);
        lenient().when(stock.getStockName()).thenReturn(stockName);
        return stock;
    }
}
