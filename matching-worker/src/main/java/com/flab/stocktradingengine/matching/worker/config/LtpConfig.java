package com.flab.stocktradingengine.matching.worker.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.flab.stocktradingengine.matching.disruptor.io.MatchListener;
import com.flab.stocktradingengine.matching.worker.ltp.LastTradedPriceWriter;
import com.flab.stocktradingengine.matching.worker.ltp.LtpRecordingMatchListener;
import com.flab.stocktradingengine.matching.worker.messaging.AccountFillPublisher;

/**
 * v2 매칭 워커가 체결가를 Redis 최근 체결가(LTP)에 기록하게 한다(v1 {@code MatchingConsumer}와 같은 의미).
 * 매칭 엔진에는 {@link AccountFillPublisher}를 감싼 listener가 주입되고, Redis 기록은 전용 스레드가 맡는다.
 */
@Configuration
public class LtpConfig {

    @Bean(initMethod = "start", destroyMethod = "close")
    public LastTradedPriceWriter lastTradedPriceWriter(
            StringRedisTemplate stringRedisTemplate,
            @Value("${matching.worker.ltp.flush-interval-millis:100}") long flushIntervalMillis) {
        return new LastTradedPriceWriter(new LastTradedPriceWriter.RedisLtpSink(stringRedisTemplate), flushIntervalMillis);
    }

    @Bean
    @Primary
    public MatchListener ltpRecordingMatchListener(AccountFillPublisher accountFillPublisher, LastTradedPriceWriter writer) {
        return new LtpRecordingMatchListener(accountFillPublisher, writer);
    }
}
