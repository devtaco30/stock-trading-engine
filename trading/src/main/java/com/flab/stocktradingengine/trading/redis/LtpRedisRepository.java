package com.flab.stocktradingengine.trading.redis;

import java.math.BigDecimal;

import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import com.flab.stocktradingengine.redis.RedisKeys;

import lombok.RequiredArgsConstructor;

/**
 * 최근 체결가(LTP) Redis 저장소 — trading 모듈 (쓰기 전용).
 *
 * <p>matching 또는 order-processor 프로세스에서만 활성화.
 * api 모듈의 동명 빈(읽기 전용)과 충돌을 방지한다.</p>
 */
@Profile({"matching", "order-processor"})
@Repository
@RequiredArgsConstructor
public class LtpRedisRepository {

    private final StringRedisTemplate stringRedisTemplate;

    public void set(String stockCode, BigDecimal price) {
        stringRedisTemplate.opsForValue().set(RedisKeys.ltp(stockCode), price.toPlainString());
    }
}
