package com.flab.stocktradingengine.matching.redis;

import java.math.BigDecimal;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import com.flab.stocktradingengine.redis.RedisKeys;

import lombok.RequiredArgsConstructor;

/**
 * 최근 체결가(LTP) Redis 저장소 — matching-engine (쓰기 전용).
 */
@Repository
@RequiredArgsConstructor
public class LtpRedisRepository {

    private final StringRedisTemplate stringRedisTemplate;

    public void set(String stockCode, BigDecimal price) {
        stringRedisTemplate.opsForValue().set(RedisKeys.ltp(stockCode), price.toPlainString());
    }
}
