package com.flab.stocktradingengine.matching.worker.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.flab.stocktradingengine.matching.disruptor.io.MatchListener;
import com.flab.stocktradingengine.matching.worker.ltp.LastTradedPriceWriter;
import com.flab.stocktradingengine.matching.worker.ltp.LtpRecordingMatchListener;
import com.flab.stocktradingengine.matching.worker.ltp.OrderbookRecordingMatchListener;
import com.flab.stocktradingengine.matching.worker.ltp.OrderbookSnapshotWriter;
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

    @Bean(initMethod = "start", destroyMethod = "close")
    public OrderbookSnapshotWriter orderbookSnapshotWriter(
            StringRedisTemplate stringRedisTemplate,
            @Value("${matching.worker.orderbook.flush-interval-millis:100}") long flushIntervalMillis) {
        return new OrderbookSnapshotWriter(new OrderbookSnapshotWriter.RedisSnapshotSink(stringRedisTemplate), flushIntervalMillis);
    }

    /** 안쪽 decorator: 계좌 발신을 감싸 체결가를 LTP 기록기에 넘긴다. 엔진에는 직접 주입되지 않는다. */
    @Bean
    public LtpRecordingMatchListener ltpRecordingMatchListener(AccountFillPublisher accountFillPublisher, LastTradedPriceWriter writer) {
        return new LtpRecordingMatchListener(accountFillPublisher, writer);
    }

    /** 바깥 decorator: 엔진에 주입되는 최종 listener. 호가창 변경을 받아 스냅샷 기록기에 넘긴다. */
    @Bean
    @Primary
    public MatchListener orderbookRecordingMatchListener(LtpRecordingMatchListener ltpRecordingMatchListener, OrderbookSnapshotWriter writer) {
        return new OrderbookRecordingMatchListener(ltpRecordingMatchListener, writer);
    }
}
