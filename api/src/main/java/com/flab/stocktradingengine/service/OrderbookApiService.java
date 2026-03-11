package com.flab.stocktradingengine.service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flab.stocktradingengine.dto.market.OrderbookDto;
import com.flab.stocktradingengine.dto.market.OrderbookLevelDto;
import com.flab.stocktradingengine.trading.redis.RedisKeys;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 호가창 조회 API용 서비스.
 * <p>Redis에 저장된 MatchingConsumer의 호가창 스냅샷을 읽어 반환한다.
 * 체결 또는 주문 이벤트 처리 후 스냅샷이 갱신되며, 갱신 전까지는 직전 상태를 반환한다.</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderbookApiService {

    private final StringRedisTemplate stringRedisTemplate;
    private final ObjectMapper objectMapper;

    /**
     * 종목 호가창 조회. bids는 가격 내림차순, asks는 가격 오름차순.
     * Redis에 스냅샷이 없으면 빈 호가창을 반환한다.
     */
    public OrderbookDto getOrderbook(String stockCode) {
        String json = stringRedisTemplate.opsForValue().get(RedisKeys.orderbook(stockCode));
        if (json == null) {
            return OrderbookDto.builder()
                .stockCode(stockCode)
                .bids(List.of())
                .asks(List.of())
                .build();
        }

        List<OrderbookLevelDto> bids;
        List<OrderbookLevelDto> asks;
        try {
            JsonNode root = objectMapper.readTree(json);
            bids = parseLevels(root.get("bids"));
            asks = parseLevels(root.get("asks"));
        } catch (Exception e) {
            log.warn("[호가창 파싱 실패] stockCode={} error={}", stockCode, e.getMessage());
            bids = List.of();
            asks = List.of();
        }

        return OrderbookDto.builder()
            .stockCode(stockCode)
            .bids(bids)
            .asks(asks)
            .build();
    }

    private static List<OrderbookLevelDto> parseLevels(JsonNode levels) {
        if (levels == null || levels.isEmpty()) return List.of();
        List<OrderbookLevelDto> result = new ArrayList<>();
        for (JsonNode level : levels) {
            result.add(OrderbookLevelDto.builder()
                .price(new BigDecimal(level.get("price").asText()))
                .quantity(level.get("quantity").asInt())
                .build());
        }
        return result;
    }
}
