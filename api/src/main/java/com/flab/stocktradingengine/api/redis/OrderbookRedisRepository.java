package com.flab.stocktradingengine.api.redis;

import java.util.Optional;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import com.flab.stocktradingengine.redis.RedisKeys;
import lombok.RequiredArgsConstructor;

/**
 * 호가창 스냅샷 Redis 저장소 — api 모듈 (읽기 전용).
 */
@Repository
@RequiredArgsConstructor
public class OrderbookRedisRepository {

    private final StringRedisTemplate stringRedisTemplate;

    public Optional<String> get(String stockCode) {
        String json = stringRedisTemplate.opsForValue().get(RedisKeys.orderbook(stockCode));
        return Optional.ofNullable(json);
    }
}
