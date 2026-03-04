package com.flab.stocktradingengine.service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import com.flab.stocktradingengine.dto.market.OrderbookDto;
import com.flab.stocktradingengine.dto.market.OrderbookLevelDto;
import com.flab.stocktradingengine.trading.matching.OrderBook;
import com.flab.stocktradingengine.trading.matching.OrderBookRegistry;

import lombok.RequiredArgsConstructor;

/**
 * 호가창 조회 API용 서비스.
 * <p>인메모리 OrderBook에서 직접 가격 레벨을 집계해 반환한다.
 * DB 조회 없이 실시간 상태를 반영하며, 가격 레벨 기준으로 정확한 개수를 보장한다.</p>
 */
@Service
@RequiredArgsConstructor
public class OrderbookApiService {

    private static final int DEFAULT_LEVELS = 10;

    private final OrderBookRegistry orderBookRegistry;

    /**
     * 종목 호가창 조회. bids는 가격 내림차순, asks는 가격 오름차순.
     * 해당 종목의 OrderBook이 없으면 빈 호가창을 반환한다.
     */
    public OrderbookDto getOrderbook(String stockCode) {
        OrderBook book = orderBookRegistry.get(stockCode);

        if (book == null) {
            return OrderbookDto.builder()
                .stockCode(stockCode)
                .bids(List.of())
                .asks(List.of())
                .build();
        }

        List<OrderbookLevelDto> bids = toLevelDtos(book.getBidLevels(DEFAULT_LEVELS));
        List<OrderbookLevelDto> asks = toLevelDtos(book.getAskLevels(DEFAULT_LEVELS));

        return OrderbookDto.builder()
            .stockCode(stockCode)
            .bids(bids)
            .asks(asks)
            .build();
    }

    private static List<OrderbookLevelDto> toLevelDtos(List<Map.Entry<BigDecimal, Integer>> levels) {
        return levels.stream()
            .map(e -> OrderbookLevelDto.builder()
                .price(e.getKey())
                .quantity(e.getValue())
                .build())
            .toList();
    }
}
