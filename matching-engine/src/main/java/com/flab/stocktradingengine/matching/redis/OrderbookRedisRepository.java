package com.flab.stocktradingengine.matching.redis;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flab.stocktradingengine.redis.RedisKeys;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 호가창 스냅샷 Redis 저장소 — matching-engine (쓰기 전용).
 * <p>클라이언트 조회용 스냅샷의 직렬화 포맷(Level·Snapshot)과 JSON 직렬화를 이 저장소가 소유한다.
 * (컨슈머는 매칭 처리만 담당하고 직렬화 포맷을 알지 않는다.)</p>
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class OrderbookRedisRepository {

    private final StringRedisTemplate stringRedisTemplate;
    private final ObjectMapper objectMapper;

    /**
     * 호가창 상위 N개 레벨(매수·매도)을 스냅샷 JSON으로 직렬화해 저장한다.
     * 직렬화 실패 시 저장을 건너뛰고 경고만 남긴다 — 스냅샷은 클라이언트 조회용 보조 데이터라 매칭 흐름을 막지 않는다.
     */
    public void saveSnapshot(String stockCode,
                             List<Map.Entry<BigDecimal, Integer>> bids,
                             List<Map.Entry<BigDecimal, Integer>> asks) {
        List<Level> bidLevels = bids.stream().map(e -> new Level(e.getKey(), e.getValue())).toList();
        List<Level> askLevels = asks.stream().map(e -> new Level(e.getKey(), e.getValue())).toList();
        try {
            String json = objectMapper.writeValueAsString(new Snapshot(bidLevels, askLevels));
            stringRedisTemplate.opsForValue().set(RedisKeys.orderbook(stockCode), json);
        } catch (JsonProcessingException e) {
            log.warn("[호가창 직렬화 실패] stockCode={}", stockCode, e);
        }
    }

    private record Level(BigDecimal price, int quantity) {}

    private record Snapshot(List<Level> bids, List<Level> asks) {}
}
