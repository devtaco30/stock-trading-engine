package com.flab.stocktradingengine.service;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.dto.market.OrderbookDto;
import com.flab.stocktradingengine.dto.market.OrderbookLevelDto;
import com.flab.stocktradingengine.trading.entity.Order;
import com.flab.stocktradingengine.trading.service.OrderMatchingQueryService;

import lombok.RequiredArgsConstructor;

/**
 * 호가창 조회 API용 서비스.
 * <p>PENDING 주문을 가격별로 집계해 매수/매도 호가 목록을 반환한다.</p>
 */
@Service
@RequiredArgsConstructor
public class OrderbookApiService {

    private final OrderMatchingQueryService orderMatchingQueryService;

    /**
     * 종목 호가창 조회. bids는 가격 내림차순, asks는 가격 오름차순.
     */
    @Transactional(readOnly = true)
    public OrderbookDto getOrderbook(String stockCode) {
        List<Order> pendingBuys = orderMatchingQueryService.getBestPendingBuys(stockCode);
        List<Order> pendingSells = orderMatchingQueryService.getBestPendingSells(stockCode);

        List<OrderbookLevelDto> bids = aggregateByPrice(pendingBuys, Comparator.comparing(Map.Entry<BigDecimal, Integer>::getKey).reversed());
        List<OrderbookLevelDto> asks = aggregateByPrice(pendingSells, Comparator.comparing(Map.Entry<BigDecimal, Integer>::getKey));

        return OrderbookDto.builder()
            .stockCode(stockCode)
            .bids(bids)
            .asks(asks)
            .build();
    }

    private static List<OrderbookLevelDto> aggregateByPrice(
        List<Order> orders,
        Comparator<Map.Entry<BigDecimal, Integer>> keyComparator
    ) {
        Map<BigDecimal, Integer> byPrice = orders.stream()
            .filter(o -> o.getRemainingQuantity() > 0)
            .collect(Collectors.groupingBy(
                Order::getPrice,
                Collectors.summingInt(Order::getRemainingQuantity)
            ));
        return byPrice.entrySet().stream()
            .sorted(keyComparator)
            .map(e -> OrderbookLevelDto.builder()
                .price(e.getKey())
                .quantity(e.getValue())
                .build())
            .toList();
    }
}
