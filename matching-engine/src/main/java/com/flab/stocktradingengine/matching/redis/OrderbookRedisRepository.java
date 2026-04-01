package com.flab.stocktradingengine.matching.redis;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import com.flab.stocktradingengine.redis.RedisKeys;

import lombok.RequiredArgsConstructor;

/**
 * 호가창 스냅샷 Redis 저장소 — matching-engine (쓰기 전용).
 */
@Repository
@RequiredArgsConstructor
public class OrderbookRedisRepository {

    private final StringRedisTemplate stringRedisTemplate;

    public void set(String stockCode, String json) {
        stringRedisTemplate.opsForValue().set(RedisKeys.orderbook(stockCode), json);
    }
}
