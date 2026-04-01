package com.flab.stocktradingengine.api.redis;

import java.math.BigDecimal;
import java.util.Optional;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import com.flab.stocktradingengine.redis.RedisKeys;

import lombok.RequiredArgsConstructor;

/**
 * 최근 체결가(LTP) Redis 저장소 — api 모듈 (읽기 전용).
 */
@Repository
@RequiredArgsConstructor
public class LtpRedisRepository {

    private final StringRedisTemplate stringRedisTemplate;

    public Optional<BigDecimal> get(String stockCode) {
        String ltp = stringRedisTemplate.opsForValue().get(RedisKeys.ltp(stockCode));
        return ltp != null ? Optional.of(new BigDecimal(ltp)) : Optional.empty();
    }
}
