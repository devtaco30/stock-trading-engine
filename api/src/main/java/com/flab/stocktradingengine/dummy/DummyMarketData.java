package com.flab.stocktradingengine.dummy;

import com.flab.stocktradingengine.dto.market.QuoteDto;
import com.flab.stocktradingengine.dto.market.StockSearchDto;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * 시장 데이터 관련 더미 데이터
 * 
 * <p>종목 시세 조회 및 종목 검색에 대한 더미 데이터를 제공합니다.
 */
public class DummyMarketData {

    /**
     * 현재가 조회 더미 데이터
     * 
     * <p>지원하는 종목:
     * <ul>
     *   <li>005930: 삼성전자 (현재가 70,000원)</li>
     *   <li>035720: 카카오 (현재가 50,000원)</li>
     * </ul>
     * 
     * @param stockCode 종목 코드 (6자리 숫자)
     * @return 시세 정보 (현재가, 전일종가, 등락률, 시가, 고가, 저가, 거래량), 존재하지 않는 종목인 경우 null
     */
    public static QuoteDto getQuote(String stockCode) {
        return switch (stockCode) {
            case "005930" -> QuoteDto.builder()  // 삼성전자
                .stockCode("005930")
                .stockName("삼성전자")
                .currentPrice(new BigDecimal("70000"))     // currentPrice: 현재가
                .previousClose(new BigDecimal("69500"))     // previousClose: 전일종가
                .changeRate(new BigDecimal("0.72"))      // changeRate: 등락률 (%)
                .open(new BigDecimal("69800"))     // open: 시가
                .high(new BigDecimal("70500"))     // high: 고가
                .low(new BigDecimal("69500"))     // low: 저가
                .volume(1500000L)                      // volume: 거래량
                .build();
            case "035720" -> QuoteDto.builder()  // 카카오
                .stockCode("035720")
                .stockName("카카오")
                .currentPrice(new BigDecimal("50000"))     // currentPrice: 현재가
                .previousClose(new BigDecimal("49500"))     // previousClose: 전일종가
                .changeRate(new BigDecimal("1.01"))      // changeRate: 등락률 (%)
                .open(new BigDecimal("49800"))     // open: 시가
                .high(new BigDecimal("50200"))     // high: 고가
                .low(new BigDecimal("49500"))     // low: 저가
                .volume(800000L)                       // volume: 거래량
                .build();
            default -> null;
        };
    }

    /**
     * 종목 검색 더미 데이터
     * 
     * <p>키워드에 따라 매칭되는 종목을 반환합니다.
     * 지원하는 종목:
     * <ul>
     *   <li>삼성전자 (005930): "삼성", "005930", "samsung"</li>
     *   <li>카카오 (035720): "카카오", "035720", "kakao"</li>
     *   <li>SK하이닉스 (000660): "하이닉스", "sk", "000660"</li>
     *   <li>네이버 (035420): "네이버", "naver", "035420"</li>
     * </ul>
     * 
     * @param keyword 검색 키워드 (종목명, 종목 코드, 영문명 등)
     * @return 검색 결과 목록 (종목 코드, 종목명, 현재가), 키워드가 없거나 매칭되는 종목이 없으면 빈 리스트
     */
    public static List<StockSearchDto> searchStocks(String keyword) {
        List<StockSearchDto> results = new ArrayList<>();
        
        if (keyword == null || keyword.isEmpty()) {
            return results;
        }
        
        String lowerKeyword = keyword.toLowerCase();
        
        // 삼성전자
        if (lowerKeyword.contains("삼성") || lowerKeyword.contains("005930") || lowerKeyword.contains("samsung")) {
            results.add(StockSearchDto.builder()
                .stockCode("005930")
                .stockName("삼성전자")
                .currentPrice(new BigDecimal("70000"))
                .build());
        }
        
        // 카카오
        if (lowerKeyword.contains("카카오") || lowerKeyword.contains("035720") || lowerKeyword.contains("kakao")) {
            results.add(StockSearchDto.builder()
                .stockCode("035720")
                .stockName("카카오")
                .currentPrice(new BigDecimal("50000"))
                .build());
        }
        
        // SK하이닉스
        if (lowerKeyword.contains("하이닉스") || lowerKeyword.contains("sk") || lowerKeyword.contains("000660")) {
            results.add(StockSearchDto.builder()
                .stockCode("000660")
                .stockName("SK하이닉스")
                .currentPrice(new BigDecimal("120000"))
                .build());
        }
        
        // 네이버
        if (lowerKeyword.contains("네이버") || lowerKeyword.contains("naver") || lowerKeyword.contains("035420")) {
            results.add(StockSearchDto.builder()
                .stockCode("035420")
                .stockName("네이버")
                .currentPrice(new BigDecimal("180000"))
                .build());
        }
        
        return results;
    }
}
