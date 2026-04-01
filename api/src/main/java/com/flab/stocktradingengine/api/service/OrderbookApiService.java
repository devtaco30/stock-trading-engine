package com.flab.stocktradingengine.api.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flab.stocktradingengine.api.dto.market.OrderbookDto;
import com.flab.stocktradingengine.api.dto.market.OrderbookLevelDto;
import com.flab.stocktradingengine.api.redis.OrderbookRedisRepository;

import lombok.RequiredArgsConstructor;

/**
 * 호가창 조회 API용 서비스.
 * <p>Redis에 저장된 MatchingConsumer의 호가창 스냅샷을 읽어 반환한다.
 * 체결 또는 주문 이벤트 처리 후 스냅샷이 갱신되며, 갱신 전까지는 직전 상태를 반환한다.</p>
 */
@Service
@RequiredArgsConstructor
public class OrderbookApiService {

    private final OrderbookRedisRepository orderbookRedisRepository;
    private final ObjectMapper objectMapper;

    /**
     * 종목 호가창 조회. bids는 가격 내림차순, asks는 가격 오름차순.
     * Redis에 스냅샷이 없으면 빈 호가창을 반환한다.
     */
    public OrderbookDto getOrderbook(String stockCode) {
        // Redis에서 호가창 조회
        String json = orderbookRedisRepository.get(stockCode).orElse(null);

        // Redis에 호가창 없으면 빈 호가창 반환
        if (json == null) {
            return OrderbookDto.builder()
                .stockCode(stockCode)
                .bids(List.of())
                .asks(List.of())
                .build();
        }

        // JSON → 타입 바인딩
        // Redis 저장 형식 (MatchingConsumer.Snapshot):
        // {"bids":[{"price":70000,"quantity":10},...], "asks":[{"price":71000,"quantity":5},...]}
        try {
            Snapshot snapshot = objectMapper.readValue(json, Snapshot.class);
            return OrderbookDto.builder()
                .stockCode(stockCode)
                .bids(snapshot.bids())
                .asks(snapshot.asks())
                .build();
        } catch (Exception e) {
            // TODO: 알람 연동 (Slack/PagerDuty 등) — 파싱 실패는 데이터 손상 또는 스키마 불일치 징후
            throw new RuntimeException("호가창 데이터 파싱 실패: " + stockCode, e);
        }
    }

    private record Snapshot(List<OrderbookLevelDto> bids, List<OrderbookLevelDto> asks) {}
}
